BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
sudo docker rm --force bucketlogging
if [ -x "$BASEDIR/bucketlogging" ]; then 
    sudo docker run -d --name bucketlogging \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:/work \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.20 \
			 journeymidnight/yig /work/bucketlogging
    echo "started bucketlogging from local dir"
fi
