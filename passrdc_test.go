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

package main

import (
	"dupe-checker/env"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_HandleFcrepoBaseUri(t *testing.T) {
	environment := env.New()
	defaultBaseUri := environment.FcrepoBaseUri

	// establish that the env var does not exist
	if existingVal, exists := os.LookupEnv(env.FCREPO_BASE_URI); exists {
		if err := os.Unsetenv(env.FCREPO_BASE_URI); err != nil {
			assert.Failf(t, "Can't unset existing value for %s, %s", env.FCREPO_BASE_URI, existingVal)
		}
	}

	// the cli provides a starturi that is equal to the default value of the env var
	assert.Nil(t, handleFcrepoBaseUri(defaultBaseUri))
	assertAndUnset(t, env.FCREPO_BASE_URI, defaultBaseUri)

	// the cli provides a starturi that is subordinate to the the default value of the env var
	assert.Nil(t, handleFcrepoBaseUri(fmt.Sprintf("%s/%s", defaultBaseUri, "users")))
	assertAndUnset(t, env.FCREPO_BASE_URI, defaultBaseUri)

	// the cli provides a starturi that is not subordinate or equal to the the default value of the env var
	assert.NotNil(t, handleFcrepoBaseUri("http://some/other/fcrepo"))
	assertNotSet(t, env.FCREPO_BASE_URI)

	assert.NotNil(t, handleFcrepoBaseUri("  "))
	assertNotSet(t, env.FCREPO_BASE_URI)

	assert.NotNil(t, handleFcrepoBaseUri("a value that's not a valid http.URL"))
	assertNotSet(t, env.FCREPO_BASE_URI)

}

func assertNotSet(t *testing.T, envVarName string) {
	if val, exists := os.LookupEnv(envVarName); exists {
		assert.Failf(t, "%s not expected to be set, has value %s", envVarName, val)
	}
}

// Verifies that the environment variable envVarName has the expected value, and then unsets the env var
func assertAndUnset(t *testing.T, envVarName, expectedVal string) {
	var actualVal string
	var exists bool

	actualVal, exists = os.LookupEnv(envVarName)

	assert.True(t, exists)
	assert.Equal(t, expectedVal, actualVal)

	unset(t, envVarName)

	assertNotSet(t, envVarName)
}

func unset(t *testing.T, envVarName string) {
	if err := os.Unsetenv(envVarName); err != nil {
		assert.Failf(t, "Can't unset existing value for %s, %s: %s", envVarName, os.Getenv(envVarName), err.Error())
	}

	if actualVal, exists := os.LookupEnv(envVarName); exists {
		assert.Failf(t, "Can't unset existing value for %s, %s", envVarName, actualVal)
	}
}
