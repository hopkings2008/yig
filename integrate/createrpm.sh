usage() {
   echo "$0 (luminous|nautilus) wocloud-version"
}

CEPH_VERSION=""
if [[ $1 != "luminous" ]] && [[ $1 != "nautilus" ]]; then
    usage
    exit
else
    CEPH_VERSION=$1
fi
if [ ! -n "$2" ]; then
    usage
    exit
fi

echo "ceph version is $CEPH_VERSION"
echo "wocloud version mark is $2"
echo "make rpm..."
BASEDIR=$(dirname $(pwd))
if [[ $CEPH_VERSION == "luminous" ]]; then
    sudo docker run --rm -v ${BASEDIR}:/work -w /work journeymidnight/yig bash -c "go env -w GO111MODULE=on; go env -w GOPROXY=https://goproxy.cn,direct; bash package/rpmbuild.sh $2"
else
    sudo docker run --rm -v ${BASEDIR}:/work -w /work hopkings2005/yigs3-ceph14:v1.0.0 bash -c "go env -w GO111MODULE=on; go env -w GOPROXY=https://goproxy.cn,direct; bash package/rpmbuild.sh  $2"
fi
