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
    echo "BEG: $FUNCNAME"

    git config --global user.email "travis@travis-ci.org"
    git config --global user.name "Travis CI"

    echo "END: $FUNCNAME"
}

export STAGING_BRANCH="${TRAVIS_BRANCH}-staging"

check_out_staging_branch() {
    echo "BEG: $FUNCNAME"

    #echo "getpopids url: ${FILE_URL_GET_POP_IDS}"
    git --version
    echo "branch is ${TRAVIS_BRANCH}"
    git status
    echo "CHECKING OUT ${STAGING_BRANCH}"
    mkdir -p tmp/wtree

    rm -rf "tmp/wtree/${STAGING_BRANCH}"
    git worktree prune

    git remote rm origin-me || true
    git remote add origin-me "https://${GH_TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git"
    git branch -D "${STAGING_BRANCH}" || true
    git fetch origin-me "${STAGING_BRANCH}" || \
	(git branch ${STAGING_BRANCH}&& \
	     git push --set-upstream origin-me ${STAGING_BRANCH} && git fetch origin-me ${STAGING_BRANCH})

    git worktree add --track -b ${STAGING_BRANCH} tmp/wtree/${STAGING_BRANCH} origin-me/${STAGING_BRANCH}
    #git merge "${TRAVIS_BRANCH}"
    pushd "tmp/wtree/${STAGING_BRANCH}"
    echo "REMOTES:"
    git remote -vv
    echo "BRANCHES:"
    git branch -vval
    #git branch --set-upstream-to=origin-me/${STAGING_BRANCH} ${STAGING_BRANCH}
    git pull
    git checkout ${TRAVIS_COMMIT} .

    echo "END: $FUNCNAME"
}

commit_staged_files() {
    echo "BEG: $FUNCNAME"

    #cp ${TRAVIS_BUILD_DIR}/*.py ${TRAVIS_BUILD_DIR}/*.wdl .
    sed -i "s#\"./#\"https://raw.githubusercontent.com/${TRAVIS_REPO_SLUG}/${TRAVIS_COMMIT}/#g" *.wdl *.wdl.json
    git status
    git diff
    git add .
    git diff --cached
    git commit -m "replaced file paths with github URLs under git commit ${TRAVIS_COMMIT}" || true
    git status
    echo "LAST COMMITS:"
    git log -3
    git show $(git rev-parse HEAD)

    echo "END: $FUNCNAME"
}

upload_files() {
    echo "BEG: $FUNCNAME"

    if [ -z ${GH_TOKEN+x} ]; then
	echo "GH_TOKEN is unset, not pushing"
    else
	git push
    fi
    git status

    echo "END: $FUNCNAME"
}

clean_up() {
    echo "BEG: $FUNCNAME"

    popd
    git status
    git worktree list
    #git worktree remove "tmp/wtree/${STAGING_BRANCH}"
    #git worktree prune
    #git worktree list
    #git status

    echo "END: $FUNCNAME"
}

setup_git
check_out_staging_branch
commit_staged_files
upload_files
clean_up

echo
echo "---------------"
echo "END SCRIPT: $0"
echo "---------------"

#git remote add me 
#git --set-upstream push

# if [[ "${LANGUAGE}" == "cwl" ]]; then
#     cwltool --non-strict Dockstore.cwl test.json
# elif [[ "${LANGUAGE}" == "wdl" ]]; then
#     java -jar cromwell-50.jar run Dockstore.wdl --inputs test.wdl.json
# elif [[ "${LANGUAGE}" == "nfl" ]]; then
#     nextflow run main.nf
# fi
