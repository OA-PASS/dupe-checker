# PASS Resource Duplicate Checker (PASS-RDC)

The PASS Resource Duplicate Checker traverses a Fedora repository containing PASS resources, and for each resource, determines if a duplicate exists by performing queries of the Elastic Search index.  Results from the process are persisted in a local Sqlite database.  The identity properties used for determining duplication are documented below.

Design-wise, PASS-RDC could have (should have?) been coded to work primarily with the index, using it as a source of truth.  Since the index does not contain auditing information like dates and creators, the duplicates would need to be retrieved from Fedora at the end of the process to fill in those necessary details used for analysis.  A future TODO :)

## Known Issues and TODOs

Presented in approximate priority order with respect to resolution.

### Queries that target multivalued fields require all values be present to be considered a duplicate

For example, given a `Journal` with two ISSNs, a query for duplicate journals would _require_ the duplicate to have both ISSNs.  Consequently, a potential duplicate that possesses only one of the ISSNs in question would _not_ be found by this query.  This issue affects queries for the `Journal` (`issn`), `Submission` (`preparers`), and `User` (`locatorIds`) type: fields that allow multiple values are affected by this issue.

For example, given the two journals below, Journal A and B are equivalent: they have the same name, and share at least one ISSN.
* Journal A, `name`: "Journal of Foo", `issn`: Print:ABCD-1234, Online:WXYZ-6789
* Journal B, `name`: "Journal of Foo", `issn`: Print:ABCD-1234

PASS-RDC has a shortcoming: the Elastic Search query for Journal A will require a match on both of its ISSNs, and will not discover Journal B.  The Elastic Search query for Journal B will require a match on its single ISSN, and **will** match Journal A.  This implementation shortcoming should not affect the ultimate result (after all, the query for Journal B correctly identifies the duplicate), but ought to be fixed to be more robust.

Conceivably this issue will cause a problem where there are more than two permutations of a multi-valued attribute.  Let's assume PASS is updated to support Linking ISSNs along with Print and Electronic ISSNs.  The following is a contrived, but feasible situation:
* Journal A, `name`: "Journal of Foo", `issn`: Print:ABCD-1234, Online:WXYZ-6789
* Journal B, `name`: "Journal of Foo", `issn`: Print:ABCD-1234, Linking:JKLM-8765

In this scenario, the duplicate Journals will not be discovered, because the query for Journal A will insist on a match for the Print and Online ISSN, while the query for Journal B will insist on a match for the Print and Linking ISSN.  Since neither Journal has the full complement of ISSNs, they won't be discovered by their respective queries.  

User resources might also be affected by this issue, where there may be up to three values contained in `locatorIds`.  Some `User` resources possess two values for `locatorIds` (missing a `hopkinsId`), while others possess all three.  I haven't seen any User resources with only one value for `locatorIds`. 

### Pausing and resuming is not implemented

While the persistence layer and associated models support the ability to resume, the production code does not persist the progress of PASS-RDC in the local database.  Completing the resume implementation was de-prioritized since the process runs reliably and within a reasonable amount of time.  Finishing the implementation is a TODO.

### Sqlite database lifecycle management is limited

PASS-RDC does very little with respect to the management of the Sqlite database used to store the results of its analysis.  If a database is present at the supplied DSN, it is used.  If a database is not present, it will attempt to be created by default, unless otherwise instructed (e.g. by supplying a `mode` to a [URI DSN](https://www.sqlite.org/uri.html), see the documentation for [opening a new connection](https://www.sqlite.org/c3ref/open.html) and the [URI format](https://www.sqlite.org/uri.html)).  After running PASS-RDC, move the database for safekeeping; PASS-RDC does not make any accommodations for re-using a previously initialized database (when resumption is implemented, using an existing database will be explicitly supported). 

It is likely that attempting to re-use an existing database will result in a slew of table constraint violations.

### Query plans may not be modified at runtime

PASS-RDC comes packaged with an internal query plan that cannot be modified at runtime.

### Query plans only support boolean OR logic

Query plans support boolean OR processes when executing and analyzing Elastic Search results.  Other boolean operators (e.g. AND or NOT) are not supported at this time.

## Requirements and Running

PASS-RDC must be executed against a running instance of Fedora and Elastic Search.  It assumes the index is the source of truth for the presence of duplicate objects, therefore the index must be complete and uncorrupted.  That is, PASS-RDC is not meant to assess the integrity of the index, but the integrity of the PASS resources contained in the Fedora repository.

Storage requirements are modest: less than 1 MiB for storing duplicate information, and less than 500 GiB if full state supporting resumption is persisted.

### Usage

`./passrdc -starturi <base repository uri> -indexuri <base elasticsearch uri> -dsn <sqlite dsn>`

Example: `./passrdc -starturi http://fcrepo:8080/fcrepo/rest -indexuri http://elasticsearch:9200/pass/_search -dsn file:/passrdc.db?mode=rwc`

## Overview

Invoking `passrdc` will launch parallel goroutines to walk the resources in the Fedora repository, starting from the URI specified by `-starturi`.

> PASS-RDC can be invoked on a subset of the repository, e.g. only check for duplicate users by providing `http://fcrepo:8080/fcrepo/rest/users` as the `-starturi` 

As repository resources are encountered, a _query plan_ will be executed against the Elastic Search index based on the [PASS type](https://github.com/OA-PASS/pass-data-model/#model-objects) of the resource.  Depending on the query plan for a given resource, multiple queries may be performed against the index for a single repository resource. Query plans are based on the [type](https://github.com/OA-PASS/pass-data-model/#model-objects) of PASS resource, and informed by the _identity properties_ documented below.

> Query plans are JSON documents that enumerate the identity properties of a given type, associate them with an Elastic Search query, and support simple boolean operations between query results

The results of the query plan execution are aggregated, then filtered, with suspected duplicates being persisted in the database.

> If a query only returns one result, then the object does not have a duplicate, so the result is not useful for analysis and is discarded

## Identity Properties

The identity properties of PASS resources were originally documented [here](https://docs.google.com/document/d/1-PimTlCdhTE5om2BGN4TOtzheLqkvO-ZdT9MAe-8OKU/edit#), but they are reproduced below with an additional commentary with respect to their usage in PASS-RDC.  PASS-RDC uses these identity properties to determine if two different resources are equivalent.  For example, if two `Journal` resources have the exact same `title` and have at least one `issn` in common, then they would be considered equivalent - i.e. a duplicate `Journal` exists.

PASS-RDC does **not** check the following object types for duplication:
* `Institution`, `Policy`, `Repository`: these objects are loaded manually by application administrators.  It is assumed that the manual loading process does not introduce duplicate objects.
* `SubmissionEvent`, `File`, `Deposit`: these objects are created during the lifecycle of the submission process, and are within the scope of a single `Submission`.  It is assumed that if a duplicate `Submission` is found, the objects associated with its lifecycle may be remediated.
* `Contributor`: this object is associated with a `Publication`; if the `Publication` is deemed a duplicate, the associated `Contributor` may be remediated.

Object types checked for duplication, and their associated identity properties are:
* Publisher
  * `name` AND `pmcParticipation`
  * `Publisher` objects are considered equal if they have the same `name` and `pmcParticipation`
* Journal
  * `nlmta` OR (`journalName` AND at least one `issn` in common)
  * `Journal` objects are considered equal if they have the same `nlmta` or if they have the same `journalName` and share at least one `issn`  
* Publication
  * (`doi` OR `pmid`) OR `title`
  * `Publication` objects are considered equal if they have the same `doi` or `pmid`, or if they have the same `title`.  
* Funder
  * `localKey`
  * `Funder` objects are considered equal if they have the same `localKey`  
* Grant
  * `localKey`
  * `Grant` objects are considered equal if they have the same `localKey`  
* RepositoryCopy
  * `accessUrl` OR (`repository`<sup>&#x2020;</sup> AND `publication`<sup>&#x2020;</sup>)
  * `RepositoryCopy` objects are considered equal if they have the same `accessUrl`, or if they have the same `repository`<sup>&#x2020;</sup> and `publication`<sup>&#x2020;</sup>.
* User
  * share at least one `locatorIds` 
  * `User` objects are considered equal if their set of `locatorIds` intersect; e.g. User A and User B only need to share one locatorId in common to be considered equivalent.
* Submission
  * `publication`<sup>&#x2020;</sup> AND (share a common `preparers`<sup>&#x2020;</sup> OR `submitter`<sup>&#x2020;</sup>)
  * `Submission` objects are considered equal if they have the same `publication`<sup>&#x2020;</sup> URI and share a common preparer or have the same submitter.

> <sup>&#x2020;</sup> values of these properties are URIs to other PASS resources

When an identity property is a URI referencing another PASS resource, _all potential equivalent resources identified by the URI need to be considered_.  For example, given two publishers where `<Foo>` denotes the URI of object Foo:
* Given `<Publication A>` and `<Publication B>` are considered equivalent (e.g. their resources have the same `doi`)
* `<Submission A>` and `<Submission B>` share a common `submitter`
  * (note that `submitter` is also a URI; in this example the `submitter` URI is identical for both submissions, so it does not need further consideration: they are byte-for-byte identical)
* `<Submission A>` `publication` references `<Publication A>` and `<Submission B>` `publication` references `<Publication B>`
  * (unlike `submitter`, the `publication` for the submissions are **not** identical)

The equivalence of `Submission A` and `Submission B` cannot be determined without knowing whether their respective Publications are equivalent.

## Components

### Model

### Persistence

### Query Plans

### Resource Retrieval

### Repository Visitor

## Developers
