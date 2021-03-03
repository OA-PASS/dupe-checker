// +build integration

package integration

import (
	"bytes"
	"embed"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http"
	"path"
	"time"
)

// returns true if the current time minus the start time is greater than the timeout duration
func timedout(start time.Time, timeout time.Duration) bool {
	isTimedOut := time.Now().Sub(start) > timeout
	return isTimedOut
}

func perform(req *http.Request, assertStatusCode int) error {
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	if res, err := httpClient.Do(req); err != nil {
		return err
	} else {
		defer res.Body.Close()
		bod := &bytes.Buffer{}
		io.Copy(bod, res.Body)
		if res.StatusCode != assertStatusCode {
			return errors.New("Error creating container, " + res.Status + ": " + bod.String())
		}
	}

	return nil
}

func testResources(shellGlob string, embeddedFs embed.FS) []fs.DirEntry {
	var matches []fs.DirEntry

	if entries, err := embeddedFs.ReadDir("."); err != nil {
		log.Fatalf("Error listing PASS test resources: %s", err.Error())
	} else {
		for _, entry := range entries {
			if matched, _ := path.Match(shellGlob, entry.Name()); matched {
				matches = append(matches, entry)
			}
		}
	}

	return matches
}
