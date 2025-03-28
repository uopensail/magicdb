#!/bin/bash -x
CURDIR=`pwd`
OS=`uname | tr '[A-Z]' '[a-z]'`
ARCH=`uname -m | tr '[A-Z]' '[a-z]'`
PROJECTNAME=$1
PUBLISHDIR=${CURDIR}/publishdir/${PROJECTNAME}


if [ x"${OS}" == x"darwin" ]; then
    MD5CMD="md5"
else
    MD5CMD="md5sum"
fi

function check_git_clean() {
    GITSTATUSOUT=`git status -s`
    if [ x${GITSTATUSOUT} == x"" ]; then
        GITCHERRYOUT=`git cherry`
        if [ x${GITCHERRYOUT} == x"" ]; then
            echo 'clean'
        else
            echo 'git_cherry_not_clean'
        fi
    else
        echo 'git_status_not_clean'
    fi
}

function write_version_info() {
    VERSIONTXTFILEPATH=${PUBLISHDIR}/version.txt
    echo "[md5 bin file]" >> ${VERSIONTXTFILEPATH}
    ${MD5CMD} ${PUBLISHDIR}/bin/${PROJECTNAME} >> ${VERSIONTXTFILEPATH}

    echo "____________________________________">> ${VERSIONTXTFILEPATH}
    echo "" >> ${VERSIONTXTFILEPATH}

    echo "[git status]" >> ${VERSIONTXTFILEPATH}
    git status >> ${VERSIONTXTFILEPATH}

    echo "____________________________________">> ${VERSIONTXTFILEPATH}
    echo "" >> ${VERSIONTXTFILEPATH}

    echo "[git log]" >> ${VERSIONTXTFILEPATH}
    git log --pretty=oneline -10 >> ${VERSIONTXTFILEPATH}
}

function targz() {
    echo ${CURDIR}
    echo "BUILD_NUMBER" ${BUILD_NUMBER}
    cd ${CURDIR}/publishdir
    rm ${PROJECTNAME}.tar.gz
    tar -cvxf ${PROJECTNAME}.tar.gz ${PROJECTNAME}
    cd -
}

function main() {
    CLEANOUT=`check_git_clean`

    if [ x"${CLEANOUT}" != x"clean" ]; then
        echo ${CLEANOUT}
        git status
        git cherry
        exit 1
    fi
    make ${PROJECTNAME}
    write_version_info
    targz
}

if [ x"${PROJECTNAME}" == x"" ]; then
    echo "please set target"
    exit 1
fi

main