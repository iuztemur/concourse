package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/concourse/concourse/atc"
	cache "github.com/patrickmn/go-cache"
	gocache "github.com/patrickmn/go-cache"
)

const algorithmLimitRows = 2

type JobNotFoundError struct {
	ID int
}

func (e JobNotFoundError) Error() string {
	return fmt.Sprintf("job ID %d is not found", e.ID)
}

type VersionsDB struct {
	Conn Conn

	Cache *gocache.Cache

	JobIDs           map[string]int
	ResourceIDs      map[string]int
	DisabledVersions map[int]map[string]bool
}

func (versions VersionsDB) VersionIsDisabled(resourceID int, versionMD5 ResourceVersion) bool {
	md5s, found := versions.DisabledVersions[resourceID]
	return found && md5s[string(versionMD5)]
}

func (versions VersionsDB) LatestVersionOfResource(resourceID int) (ResourceVersion, bool, error) {
	tx, err := versions.Conn.Begin()
	if err != nil {
		return "", false, err
	}

	defer tx.Rollback()

	version, found, err := versions.latestVersionOfResource(tx, resourceID)
	if err != nil {
		return "", false, err
	}

	if !found {
		return "", false, nil
	}

	err = tx.Commit()
	if err != nil {
		return "", false, err
	}

	return version, true, nil
}

func (versions VersionsDB) SuccessfulBuilds(paginatedBuilds *PaginatedBuilds, jobID int) {
	builder := psql.Select("b.id").
		From("builds b").
		Where(sq.Eq{
			"b.job_id": jobID,
			"b.status": "succeeded",
		}).
		OrderBy("b.id DESC")

	paginatedBuilds.passedJobsBuilder[jobID] = builder
	paginatedBuilds.column = "b.id"
	paginatedBuilds.conn = versions.Conn

	return
}

func (versions VersionsDB) SuccessfulBuildsVersionConstrained(paginatedBuilds *PaginatedBuilds, jobID int, version ResourceVersion, resourceID int) {
	builder := psql.Select("build_id").
		From("successful_build_versions").
		Where(sq.Eq{
			"job_id":      jobID,
			"version_md5": version,
			"resource_id": resourceID,
		}).
		OrderBy("build_id DESC")

	paginatedBuilds.passedJobsBuilder[jobID] = builder
	paginatedBuilds.column = "build_id"
	paginatedBuilds.conn = versions.Conn

	return
}

func (versions VersionsDB) BuildOutputs(buildID int) ([]AlgorithmOutput, error) {
	uniqOutputs := map[string]AlgorithmOutput{}
	rows, err := psql.Select("name", "resource_id", "version_md5").
		From("build_resource_config_version_inputs").
		Where(sq.Eq{"build_id": buildID}).
		RunWith(versions.Conn).
		Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var output AlgorithmOutput
		err := rows.Scan(&output.InputName, &output.ResourceID, &output.Version)
		if err != nil {
			return nil, err
		}

		uniqOutputs[output.InputName] = output
	}

	rows, err = psql.Select("name", "resource_id", "version_md5").
		From("build_resource_config_version_outputs").
		Where(sq.Eq{"build_id": buildID}).
		RunWith(versions.Conn).
		Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var output AlgorithmOutput
		err := rows.Scan(&output.InputName, &output.ResourceID, &output.Version)
		if err != nil {
			return nil, err
		}

		uniqOutputs[output.InputName] = output
	}

	outputs := []AlgorithmOutput{}
	for _, o := range uniqOutputs {
		outputs = append(outputs, o)
	}

	sort.Slice(outputs, func(i, j int) bool {
		return outputs[i].InputName > outputs[j].InputName
	})

	return outputs, nil
}

func (versions VersionsDB) SuccessfulBuildOutputs(buildID int) ([]AlgorithmOutput, error) {
	cacheKey := fmt.Sprintf("o%d", buildID)

	c, found := versions.Cache.Get(cacheKey)
	if found {
		return c.([]AlgorithmOutput), nil
	}

	uniqOutputs := map[string]AlgorithmOutput{}
	// TODO: prefer outputs over inputs for the same name
	rows, err := psql.Select("name", "resource_id", "version_md5").
		From("successful_build_versions").
		Where(sq.Eq{"build_id": buildID}).
		RunWith(versions.Conn).
		Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var output AlgorithmOutput
		err := rows.Scan(&output.InputName, &output.ResourceID, &output.Version)
		if err != nil {
			return nil, err
		}

		uniqOutputs[output.InputName] = output
	}

	outputs := []AlgorithmOutput{}
	for _, o := range uniqOutputs {
		outputs = append(outputs, o)
	}

	sort.Slice(outputs, func(i, j int) bool {
		return outputs[i].InputName > outputs[j].InputName
	})

	versions.Cache.Set(cacheKey, outputs, time.Hour)

	return outputs, nil
}

func (versions VersionsDB) FindVersionOfResource(resourceID int, v atc.Version) (ResourceVersion, bool, error) {
	versionJSON, err := json.Marshal(v)
	if err != nil {
		return "", false, nil
	}

	var version ResourceVersion
	err = psql.Select("rcv.version_md5").
		From("resource_config_versions rcv").
		Join("resources r ON r.resource_config_scope_id = rcv.resource_config_scope_id").
		Where(sq.Eq{
			"r.id": resourceID,
		}).
		Where(sq.Expr("rcv.version_md5 = md5(?)", versionJSON)).
		RunWith(versions.Conn).
		QueryRow().
		Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	return version, true, err
}

func (versions VersionsDB) LatestBuildID(jobID int) (int, bool, error) {
	var buildID int
	err := psql.Select("b.id").
		From("builds b").
		Where(sq.Eq{
			"b.job_id":    jobID,
			"b.scheduled": true,
		}).
		OrderBy("b.id DESC").
		Limit(100).
		RunWith(versions.Conn).
		QueryRow().
		Scan(&buildID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}

	return buildID, true, nil
}

func (versions VersionsDB) NextEveryVersion(buildID int, resourceID int) (ResourceVersion, bool, error) {
	tx, err := versions.Conn.Begin()
	if err != nil {
		return "", false, err
	}

	defer tx.Rollback()

	var checkOrder int
	err = psql.Select("rcv.check_order").
		From("build_resource_config_version_inputs i").
		Join("resource_config_versions rcv ON rcv.resource_config_scope_id = (SELECT resource_config_scope_id FROM resources WHERE id = ?)", resourceID).
		Where(sq.Expr("i.version_md5 = rcv.version_md5")).
		Where(sq.Eq{"i.build_id": buildID}).
		RunWith(tx).
		QueryRow().
		Scan(&checkOrder)
	if err != nil {
		if err == sql.ErrNoRows {
			version, found, err := versions.latestVersionOfResource(tx, resourceID)
			if err != nil {
				return "", false, err
			}

			if !found {
				return "", false, nil
			}

			err = tx.Commit()
			if err != nil {
				return "", false, err
			}

			return version, true, nil
		}

		return "", false, err
	}

	var nextVersion ResourceVersion
	err = psql.Select("rcv.version_md5").
		From("resource_config_versions rcv").
		Where(sq.Expr("rcv.resource_config_scope_id = (SELECT resource_config_scope_id FROM resources WHERE id = ?)", resourceID)).
		Where(sq.Expr("NOT EXISTS (SELECT 1 FROM resource_disabled_versions WHERE resource_id = ? AND version_md5 = rcv.version_md5)", resourceID)).
		Where(sq.Gt{"rcv.check_order": checkOrder}).
		OrderBy("rcv.check_order ASC").
		Limit(1).
		RunWith(tx).
		QueryRow().
		Scan(&nextVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			err = psql.Select("rcv.version_md5").
				From("resource_config_versions rcv").
				Where(sq.Expr("rcv.resource_config_scope_id = (SELECT resource_config_scope_id FROM resources WHERE id = ?)", resourceID)).
				Where(sq.Expr("NOT EXISTS (SELECT 1 FROM resource_disabled_versions WHERE resource_id = ? AND version_md5 = rcv.version_md5)", resourceID)).
				Where(sq.LtOrEq{"rcv.check_order": checkOrder}).
				OrderBy("rcv.check_order DESC").
				Limit(1).
				RunWith(tx).
				QueryRow().
				Scan(&nextVersion)
			if err != nil {
				if err == sql.ErrNoRows {
					return "", false, nil
				}
				return "", false, err
			}
		} else {
			return "", false, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return "", false, err
	}

	return nextVersion, true, nil
}

func (versions VersionsDB) LatestBuildPipes(buildID int, passedJobs map[int]bool) (map[int]int, error) {
	rows, err := psql.Select("p.from_build_id", "b.job_id").
		From("build_pipes p").
		Join("builds b ON b.id = p.from_build_id").
		Where(sq.Eq{
			"p.to_build_id": buildID,
		}).
		RunWith(versions.Conn).
		Query()
	if err != nil {
		return nil, err
	}

	jobToBuildPipes := map[int]int{}
	for rows.Next() {
		var buildID int
		var jobID int

		err = rows.Scan(&buildID, &jobID)
		if err != nil {
			return nil, err
		}

		if passedJobs[jobID] {
			jobToBuildPipes[jobID] = buildID
		}
	}

	return jobToBuildPipes, nil
}

func (versions VersionsDB) UnusedBuilds(paginatedBuilds *PaginatedBuilds, buildID int, jobID int) error {
	preBuilder := psql.Select("id").
		From("builds").
		Where(sq.And{
			sq.Gt{"id": buildID},
			sq.Eq{
				"job_id": jobID,
				"status": "succeeded",
			},
		}).
		OrderBy("id ASC")

	builder := psql.Select("id").
		From("builds").
		Where(sq.And{
			sq.LtOrEq{"id": buildID},
			sq.Eq{
				"job_id": jobID,
				"status": "succeeded",
			},
		}).
		OrderBy("id DESC")

	paginatedBuilds.passedJobsPreBuilder[jobID] = preBuilder
	paginatedBuilds.passedJobsBuilder[jobID] = builder
	paginatedBuilds.column = "id"
	paginatedBuilds.conn = versions.Conn

	return nil
}

func (versions VersionsDB) UnusedBuildsVersionConstrained(paginatedBuilds *PaginatedBuilds, buildID int, jobID int, version ResourceVersion, resourceID int) error {
	preBuilder := psql.Select("build_id").
		From("successful_build_versions").
		Where(sq.Eq{
			"job_id":      jobID,
			"version_md5": version,
			"resource_id": resourceID,
		}).
		Where(sq.Gt{
			"build_id": buildID,
		}).
		OrderBy("build_id ASC")

	builder := psql.Select("build_id").
		From("successful_build_versions").
		Where(sq.Eq{
			"job_id":      jobID,
			"version_md5": version,
			"resource_id": resourceID,
		}).
		Where(sq.LtOrEq{
			"build_id": buildID,
		}).
		OrderBy("build_id DESC")

	paginatedBuilds.passedJobsPreBuilder[jobID] = preBuilder
	paginatedBuilds.passedJobsBuilder[jobID] = builder
	paginatedBuilds.column = "build_id"
	paginatedBuilds.conn = versions.Conn

	return nil
}

func (versions VersionsDB) OrderPassedJobs(currentJobID int, jobs JobSet) ([]int, error) {
	// var jobIDs []int
	// for id, _ := range jobs {
	// 	jobIDs = append(jobIDs, id)
	// }

	// sort.Ints(jobIDs)

	// return jobIDs, nil

	type jobLatestBuild struct {
		jobID         int
		latestBuildID int
	}

	jobsLatestBuilds := []jobLatestBuild{}
	for id, _ := range jobs {
		cacheKey := fmt.Sprintf("jlb%d", id)

		var buildID sql.NullInt64
		c, found := versions.Cache.Get(cacheKey)
		if found {
			buildID.Int64 = c.(int64)
		} else {
			err := psql.Select("latest_completed_build_id").
				From("jobs").
				Where(sq.Eq{"id": id}).
				RunWith(versions.Conn).
				QueryRow().
				Scan(&buildID)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, JobNotFoundError{id}
				}
				return nil, err
			}

			versions.Cache.Set(cacheKey, buildID.Int64, cache.DefaultExpiration)
		}

		jobsLatestBuilds = append(jobsLatestBuilds, jobLatestBuild{
			jobID:         id,
			latestBuildID: int(buildID.Int64),
		})
	}

	sort.Slice(jobsLatestBuilds, func(i, j int) bool {
		return jobsLatestBuilds[i].latestBuildID < jobsLatestBuilds[j].latestBuildID
	})

	orderedJobs := []int{}
	for _, jlb := range jobsLatestBuilds {
		orderedJobs = append(orderedJobs, jlb.jobID)
	}

	return orderedJobs, nil
}

func (versions VersionsDB) latestVersionOfResource(tx Tx, resourceID int) (ResourceVersion, bool, error) {
	var scopeID int
	err := psql.Select("resource_config_scope_id").
		From("resources").
		Where(sq.Eq{"id": resourceID}).
		RunWith(tx).
		QueryRow().
		Scan(&scopeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	var version ResourceVersion
	err = psql.Select("version_md5").
		From("resource_config_versions").
		Where(sq.Eq{"resource_config_scope_id": scopeID}).
		Where(sq.Expr("version_md5 NOT IN (SELECT version_md5 FROM resource_disabled_versions WHERE resource_id = ?)", resourceID)).
		OrderBy("check_order DESC").
		Limit(1).
		RunWith(tx).
		QueryRow().
		Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	return version, true, nil
}

type PaginatedBuilds struct {
	passedJobsBuilder    map[int]sq.SelectBuilder
	passedJobsPreBuilder map[int]sq.SelectBuilder
	column               string

	passedJobsBuildIDs map[int][]int
	offset             int

	currentJob int

	conn Conn
}

func NewPaginatedBuilds() *PaginatedBuilds {
	return &PaginatedBuilds{
		passedJobsBuilder:    map[int]sq.SelectBuilder{},
		passedJobsPreBuilder: map[int]sq.SelectBuilder{},
		passedJobsBuildIDs:   map[int][]int{},
	}
}

func (bs *PaginatedBuilds) CurrentPassedJobID() int {
	return bs.currentJob
}

func (bs *PaginatedBuilds) Next(debug func(messages ...interface{}), orderedJobs []int) (int, bool, error) {
	debug("current offset", bs.offset, "build len", len(bs.passedJobsBuildIDs[bs.currentJob]))
	if bs.offset+1 > len(bs.passedJobsBuildIDs[bs.currentJob]) {
		if bs.currentJob == 0 || bs.currentJob == orderedJobs[len(orderedJobs)-1] {
			bs.currentJob = orderedJobs[0]
		} else {
			for i, job := range orderedJobs {
				if job == bs.currentJob {
					bs.currentJob = orderedJobs[i+1]
					break
				}
			}
		}
		debug("current job", bs.currentJob)

		preQuery := false
		builder := bs.passedJobsBuilder[bs.currentJob]
		if len(bs.passedJobsBuildIDs[bs.currentJob]) == 0 {
			if _, ok := bs.passedJobsPreBuilder[bs.currentJob]; ok {
				builder = bs.passedJobsPreBuilder[bs.currentJob]
				delete(bs.passedJobsPreBuilder, bs.currentJob)
				preQuery = true
			}
		} else {
			builder = builder.Where(sq.Lt{
				bs.column: bs.passedJobsBuildIDs[bs.currentJob][len(bs.passedJobsBuildIDs[bs.currentJob])-1],
			})
		}

		if !preQuery {
			builder = builder.Limit(algorithmLimitRows)
		}

		bs.passedJobsBuildIDs[bs.currentJob] = []int{}
		bs.offset = 0

		rows, err := builder.
			RunWith(bs.conn).
			Query()
		if err != nil {
			return 0, false, err
		}

		for rows.Next() {
			var buildID int

			err = rows.Scan(&buildID)
			if err != nil {
				return 0, false, err
			}

			bs.passedJobsBuildIDs[bs.currentJob] = append(bs.passedJobsBuildIDs[bs.currentJob], buildID)
		}

		debug("found", len(bs.passedJobsBuildIDs[bs.currentJob]), "builds in successful")

		if len(bs.passedJobsBuildIDs[bs.currentJob]) == 0 {
			if preQuery == true {
				return bs.Next(debug, orderedJobs)
			} else {
				return 0, false, nil
			}
		}
	}

	id := bs.passedJobsBuildIDs[bs.currentJob][bs.offset]
	bs.offset++

	return id, true, nil
}
