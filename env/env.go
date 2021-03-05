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

// Evaluates relevant environment variables and provides reasonable defaults for runtime operation
package env

import (
	"os"
	"strings"
)

type Env struct {
	// base http uri of the fedora repository rest api
	FcrepoBaseUri,
	// port that the servlet container listens on
	FcrepoPort,
	// user that has admin privileges to the fedora repository
	FcrepoUser,
	// password granting admin privileges to the fedora repository
	FcrepoPassword,
	// the directory within the docker container (perhaps mounted from a volume) used to persist fedora data
	FcrepoDataDir,
	// header set by the shibboleth service provider identifying the authenticated user,
	// used by the jetty-shib-authenticator
	FcrepoSpAuthHeader,
	// roles that shibboleth authenticated users belong to, comma delimited, used by the jetty-shib-authenticator
	FcrepoSpAuthRoles,
	// name of the basic authentication realm that protects fedora (corresponds to the realm name of the login service
	// in fedora's web.xml)
	FcrepoAuthRealm,
	// Spring Resource URI identifying the ModeShape Spring configuration
	FcrepoModeConfig,
	// log level used by Fedora
	FcrepoLogLevel,
	// log level used by the Authentication-related classes of Fedora
	FcrepoAuthLogLevel,
	// public URI of the Fedora repository rest api
	FcrepoPublicBaseUri,
	// shibboleth service provider URI that proxies the Fedora base URI
	FcrepoSpProxyUri,
	// Skips the service dependency check when starting ITs, useful for speeding up iteration when
	// services are known to be up
	ItSkipServiceDepCheck,
	// Maximum number of concurrent requests allowed to Fedora
	FcrepoMaxConcurrentRequests,

	HttpTimeoutMs,
	SqliteDsn string
}

// answers a struct containing supported environment variables
func New() Env {
	return Env{

		FcrepoBaseUri:      getEnv("${FCREPO_BASE_URI}", "http://fcrepo:8080/fcrepo/rest"),
		FcrepoPort:         getEnv("${FCREPO_JETTY_PORT}", ""),
		FcrepoUser:         getEnv("${FCREPO_USER}", "fedoraAdmin"),
		FcrepoPassword:     getEnv("${FCREPO_PASS}", "moo"),
		FcrepoDataDir:      getEnv("${FCREPO_DATA_DIR}", ""),
		FcrepoSpAuthHeader: getEnv("${FCREPO_SP_AUTH_HEADER}", ""),
		FcrepoSpAuthRoles:  getEnv("${FCREPO_SP_AUTH_ROLES}", ""),
		FcrepoAuthRealm:    getEnv("${FCREPO_AUTH_REALM}", ""),
		FcrepoModeConfig:   getEnv("${FCREPO_MODESHAPE_CONFIG}", ""),
		FcrepoLogLevel:     getEnv("${FCREPO_LOGLEVEL}", ""),
		FcrepoAuthLogLevel: getEnv("${FCREPO_AUTH_LOGLEVEL}", ""),
		// Note the following env vars are for testing only, *not* present or used in the production image
		FcrepoPublicBaseUri:         getEnv("${PUBLIC_BASE_URI}", ""),
		FcrepoSpProxyUri:            getEnv("${SP_PROXY_URI}", ""),
		FcrepoMaxConcurrentRequests: getEnv("${FCREPO_MAX_CONCURRENT_REQUESTS}", "5"),

		// Skips the service dependency check when starting ITs, useful for speeding up iteration when
		// services are known to be up
		ItSkipServiceDepCheck: getEnv("${IT_SKIP_SERVICE_DEP_CHECK}", "false"),
		HttpTimeoutMs:         getEnv("${HTTP_TIMEOUT_MS}", "600000"), // 10 minutes
		SqliteDsn:             getEnv("${SQLITE_DSN}", "file:/tmp/dupechecker.db"),
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
