#!/bin/sh

gh api graphql --paginate --slurp \
    -F owner='apify' \
    -F repo='crawlee-python' \
    -f query='
        query ($owner: String!, $repo: String!, $endCursor: String) {
            repository(owner: $owner, name: $repo) {
                pullRequests(first: 100, after: $endCursor) {
                    nodes {
                        number,
                        closingIssuesReferences(last: 100) {
                            nodes { number }
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        }
    ' | 
jq '
    [
        [.[] | .data.repository.pullRequests.nodes ] 
            | flatten[]
            | {
                (.number | tostring):
                [.closingIssuesReferences.nodes | .[] | .number]
            }
    ] | add' > pullRequestIssues.json
