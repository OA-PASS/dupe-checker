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

package process

import (
	"dupe-checker/env"
	"dupe-checker/model"
	"dupe-checker/query"
	"dupe-checker/visit"
	"errors"
	"fmt"
	"log"
	"strings"
)

type MatchHandler func(result interface{}) (bool, error)

type Processor struct {
	Plans            map[string]query.Plan
	ErrorHandler     func(e error)
	EventHandler     func(e visit.Event)
	ContainerHandler func(c model.LdpContainer)
}

func New(plans map[string]query.Plan, matchHandler MatchHandler) *Processor {
	return &Processor{
		ErrorHandler:     visit.LogErrorHandler,
		EventHandler:     visit.NoopEventHandler,
		ContainerHandler: newContainerHandler(plans, matchHandler),
	}
}

func newContainerHandler(plans map[string]query.Plan, matchHandler MatchHandler) func(container model.LdpContainer) {
	return func(c model.LdpContainer) {
		var queryPlan query.Plan

		// Select the query plan based on the type of resource
		if isPass, passType := c.IsPassResource(); isPass {
			switch passType {
			case model.PassTypeUser:
				fallthrough
			case model.PassTypePublication:
				fallthrough
			case model.PassTypeFunder:
				fallthrough
			case model.PassTypeGrant:
				fallthrough
			case model.PassTypeJournal:
				fallthrough
			case model.PassTypeRepositoryCopy:
				fallthrough
			case model.PassTypeSubmission:
				queryPlan = plans[passType]
			default:
				panic("Unsupported type " + passType)
			}

			if queryPlan == nil {
				log.Fatalf("Query Plan for %s is missing from the query plan document.", passType)
			}

		} else {
			panic(fmt.Sprintf("Container not a PASS resource %v", c))
		}

		// Execute the query and hand the result off to the match handler
		// The match handler is responsible for processing the result (i.e. determining if there are duplicates and
		// persisting them in the database).
		if _, err := queryPlan.Execute(c, matchHandler); err != nil {
			// allow for errors where keys cannot be extracted, this is to be expected with our tests
			if !errors.Is(err, query.ErrMissingRequiredKey) {
				log.Printf("Error performing query: %s", err.Error())
			}
		}
	}
}

func (p Processor) Process(startUri string, visitor *visit.Controller, environment env.Env) {

	// descend into all containers that are not pass resources or acls
	pubsAndUsersFilter := func(c model.LdpContainer) bool {
		if visit.IsAclResource(c) {
			// don't descend acl resources
			return false
		}

		if visit.IsPassResource(c) {
			// don't descend pass resources (TODO revisit this re files)
			return false
		}

		// if the container is the root container, publications container, or users container, descend
		// otherwise, don't
		switch {
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/publications", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/users", environment.FcrepoBaseUri)):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s/", environment.FcrepoBaseUri):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s", environment.FcrepoBaseUri):
			return true
		default:
			return false
		}
	}

	// descend into all containers that are not pass resources or acls
	allTheRestFilter := func(c model.LdpContainer) bool {
		if visit.IsAclResource(c) {
			// don't descend acl resources
			return false
		}

		if visit.IsPassResource(c) {
			// don't descend pass resources (TODO revisit this re files)
			return false
		}

		// if the container is the root container, funders, grants, repositoryCopies, submissions, or journals
		// container, descend.
		// otherwise, don't
		switch {
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/funders", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/grants", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/repositoryCopies", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/submissions", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/journals", environment.FcrepoBaseUri)):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s/", environment.FcrepoBaseUri):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s", environment.FcrepoBaseUri):
			return true
		default:
			return false
		}
	}

	// accept all pass resources for processing
	acceptFn := func(c model.LdpContainer) bool {
		// accept PASS resources for processing (i.e. accepted resources will be sent to the visitor.Containers channel)
		return visit.IsPassResource(c)
	}

	visitor.ContainerHandler(p.ContainerHandler)
	visitor.ErrorHandler(p.ErrorHandler)
	visitor.EventHandler(p.EventHandler)

	// process publications and users first
	visitor.Begin(startUri, acceptFn, pubsAndUsersFilter)
	// then process the rest of the resources
	visitor.Begin(startUri, acceptFn, allTheRestFilter)
}
