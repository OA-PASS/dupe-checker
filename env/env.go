//
// Copyright 2021 Johns Hopkins University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package env

import (
	"os"
	"strings"
)

const (
	FCREPO_BASE_URI                = "FCREPO_BASE_URI"
	FCREPO_USER                    = "FCREPO_USER"
	FCREPO_PASS                    = "FCREPO_PASS"
	FCREPO_MAX_CONCURRENT_REQUESTS = "FCREPO_MAX_CONCURRENT_REQUESTS"
	FCREPO_INDEX_BASE_URI          = "FCREPO_INDEX_BASE_URI"
	HTTP_TIMEOUT_MS                = "HTTP_TIMEOUT_MS"
	SQLITE_DSN                     = "SQLITE_DSN"
	INDEX_SEARCH_BASE_URI          = "INDEX_SEARCH_BASE_URI"
	INDEX_SEARCH_MAX_RESULT_SIZE   = "INDEX_SEARCH_MAX_RESULT_SIZE"
	IT_SKIP_SERVICE_DEP_CHECK      = "IT_SKIP_SERVICE_DEP_CHECK"
)

type Env struct {
	// base http uri of the fedora repository rest api
	FcrepoBaseUri,
	// indexed base uri
	FcrepoIndexBaseUri,
	// user that has admin privileges to the fedora repository
	FcrepoUser,
	// password granting admin privileges to the fedora repository
	FcrepoPassword,
	// Skips the service dependency check when starting ITs, useful for speeding up iteration when
	// services are known to be up
	ItSkipServiceDepCheck,
	// Maximum number of concurrent requests allowed to Fedora
	FcrepoMaxConcurrentRequests,
	// base http uri of the index search endpoint
	IndexSearchBaseUri,
	// maximum number of hits to allow from Elastic Search on a per-request basis
	IndexSearchMaxResultSize,
	HttpTimeoutMs,
	SqliteDsn string
}

// answers a struct containing supported environment variables
func New() Env {
	return Env{

		FcrepoBaseUri:               getEnv(FCREPO_BASE_URI, "http://fcrepo:8080/fcrepo/rest"),
		FcrepoIndexBaseUri:          getEnv(FCREPO_INDEX_BASE_URI, "http://fcrepo:8080/fcrepo/rest"),
		FcrepoUser:                  getEnv(FCREPO_USER, "fedoraAdmin"),
		FcrepoPassword:              getEnv(FCREPO_PASS, "moo"),
		FcrepoMaxConcurrentRequests: getEnv(FCREPO_MAX_CONCURRENT_REQUESTS, "5"),

		// Skips the service dependency check when starting ITs, useful for speeding up iteration when
		// services are known to be up
		ItSkipServiceDepCheck:    getEnv(IT_SKIP_SERVICE_DEP_CHECK, "false"),
		HttpTimeoutMs:            getEnv(HTTP_TIMEOUT_MS, "600000"), // 10 minutes
		SqliteDsn:                getEnv(SQLITE_DSN, "file:/tmp/dupechecker.db"),
		IndexSearchBaseUri:       getEnv(INDEX_SEARCH_BASE_URI, "http://elasticsearch:9200/pass/_search"),
		IndexSearchMaxResultSize: getEnv(INDEX_SEARCH_MAX_RESULT_SIZE, "1000"),
	}
}

func getEnv(varName, defaultValue string) string {
	varName = strings.TrimSpace(varName)
	if strings.HasPrefix(varName, "${") {
		varName = varName[2:]
	}

	if strings.HasSuffix(varName, "}") {
		varName = varName[:len(varName)-1]
	}

	if value, exists := os.LookupEnv(varName); !exists {
		return defaultValue
	} else {
		return value
	}
}
