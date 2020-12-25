#!/usr/bin/env bash

set -o errexit
set -o pipefail
#set -o nounset
set -o xtrace

#
# so, the staged version actually needs to:
#    - be checked out into a separate dir, e.g. a worktree
#    - copy the relevant files, e.g. *.py and *.wdl, from the main branch, where they have the 
#    - then replace GITCOMMIT with the right commit, and raw github file address
#    - after committing, tag with a tag that identifies the commit 
#    - possibly name the branch smth like staging/this, or even make a separate repo just for this (and make a token for writing to just that
#      repo)
#
#    - then deploy to terra.
#        - so, actually, on terra, does the actual wdl that was used get saved?
#          regardless, the file might not be.
#
#  - so, let's say we split this into several wdls, and do an import.
#    btw, let's maybe try an import?

#
# then, can add proper 
#

setup_git() {
    echo "Setting up git"
    git config --global user.email "travis@travis-ci.org"
    git config --global user.name "Travis CI"
    echo "Git setup done"
}

export STAGING_BRANCH="${TRAVIS_BRANCH}-staging"

commit_staged_files() {
    #echo "getpopids url: ${FILE_URL_GET_POP_IDS}"
    git --version
    echo "branch is ${TRAVIS_BRANCH}"
    git status
    echo "CHECKING OUT ${STAGING_BRANCH}"
    git checkout -b "${STAGING_BRANCH}"
    git merge "${TRAVIS_BRANCH}"
    sed -i "s/GITCOMMIT/${TRAVIS_COMMIT}/g" simp.wdl
    git status
    git diff
    git add *.wdl
    git diff --cached
    git commit -m 'replaced git commit'
}

upload_files() {
    git remote add origin-me https://${GH_TOKEN}@github.com/notestaff/dockstore-tool-cms2.git
    git push --set-upstream origin-me "${STAGING_BRANCH}"
    git status
    git checkout ${TRAVIS_BRANCH}
    git status
}

setup_git
commit_staged_files
upload_files

#git remote add me 
#git --set-upstream push

# if [[ "${LANGUAGE}" == "cwl" ]]; then
#     cwltool --non-strict Dockstore.cwl test.json
# elif [[ "${LANGUAGE}" == "wdl" ]]; then
#     java -jar cromwell-50.jar run Dockstore.wdl --inputs test.wdl.json
# elif [[ "${LANGUAGE}" == "nfl" ]]; then
#     nextflow run main.nf
# fi
