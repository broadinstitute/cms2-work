#!/usr/bin/env bash

set -o errexit
set -o pipefail
#set -o nounset
set -o xtrace

setup_git() {
    git config --global user.email "travis@travis-ci.org"
    git config --global user.name "Travis CI"
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
    git rm -f .travis.yaml || true
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
