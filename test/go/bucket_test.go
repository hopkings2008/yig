package _go

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_MakeBucket(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")
}

func Test_HeadBucket(t *testing.T) {
	sc := NewS3()
	err := sc.HeadBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadBucket Success.")
}

func Test_DeleteBucket(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	err = sc.HeadBucket(TEST_BUCKET)
	if err == nil {
		t.Fatal("DeleteBucket Failed")
		panic(err)
	}
	t.Log("DeleteBucket Success.")
}

func Test_ListObjectVersionsWithMaxKey(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer sc.DeleteBucket(TEST_BUCKET)

	if err = sc.PutBucketVersioning(TEST_BUCKET, "Enabled"); err != nil {
		t.Fatal("PutBucketVersioning err :", err)
		panic(err)
	}

	// TEST_KEY1 and TEST_KEY2, 3 versions for each.
	keyCount := 2
	versionCount := 3
	expectedList := make([][]string, keyCount*versionCount)
	for key := 0; key < keyCount; key++ {
		for version := 0; version < versionCount; version++ {
			versionId, err := sc.PutObjectVersioning(TEST_BUCKET, TEST_KEY+strconv.Itoa(key), TEST_VALUE)
			if err != nil {
				t.Fatal("PutObject err:", err)
				panic(err)
			}

			expectedList[key*versionCount+(versionCount-version-1)] = []string{TEST_KEY + strconv.Itoa(key), versionId}
			defer sc.DeleteObjectVersion(TEST_BUCKET, TEST_KEY+strconv.Itoa(key), versionId)
		}
	}

	keyMarker := ""
	versionIdMarker := ""
	maxKeys := int64(2)
	resultList := make([][]string, 0)
	for {
		var result *[][]string
		var isTruncated bool

		result, _, isTruncated, keyMarker, versionIdMarker, err = sc.ListObjectVersions(TEST_BUCKET, keyMarker, versionIdMarker, "", "", maxKeys)
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}
		t.Log(isTruncated, keyMarker, versionIdMarker)

		resultList = append(resultList, (*result)...)

		if !isTruncated {
			break
		}
	}

	if len(expectedList) != len(resultList) {
		t.Fatal("resultList:", resultList, "expectedList:", expectedList)
		panic("")
	}

	for i, _ := range expectedList {
		if expectedList[i][0] != resultList[i][0] || expectedList[i][1] != resultList[i][1] {
			t.Fatal("unexpected list result:", resultList)
			panic("")
		}
	}

	t.Log("ListObjectVersions Success.")
}

// ListObjects and ListObjectVersions with Delimiter.
func Test_ListObjectVersionsWithDelimiter(t *testing.T) {
	sc := NewS3()

	// 2 dir * 2 object * 2 versions + 1 object * 2 versions, maxKeys 1000, delimiter ""
	ListObjectVersionsWithDelimiterHelper(t, sc, 2, 2, 2, 1, 2, 1000, "")
	ListObjectVersionsWithDelimiterHelper(t, sc, 3, 2, 4, 1, 2, 3, "")

	ListObjectVersionsWithDelimiterHelper(t, sc, 2, 2, 2, 1, 2, 1000, "/")
	ListObjectVersionsWithDelimiterHelper(t, sc, 3, 4, 2, 1, 2, 3, "/")
	ListObjectVersionsWithDelimiterHelper(t, sc, 3, 2, 4, 2, 2, 3, "/")

	// 2 dir * 9 object * 300 versions + 1 object * 3 versions, maxKeys 1000, delimiter "".
	// ListObjectVersionsWithDelimiterHelper(t, sc, 2, 9, 300, 1, 3, 1000, "/")
	// 1001 dir * 9 object * 300 versions + 2 object * 3 versions, maxKeys 1000, delimiter "".
	// ListObjectVersionsWithDelimiterHelper(t, sc, 1001, 2, 2, 2, 3, 1000, "/")

	t.Log("All TC passed!")
}

func ListObjectVersionsWithDelimiterHelper(t *testing.T, sc *S3Client, dirNum, dirObjNum, dirObjVersionNum, objNum, versionNum, maxKeys int, delimiter string) {
	t.Log("TC: dir:", dirNum, "obj in it:", dirObjNum, "*", dirObjVersionNum, ", object:", objNum, "*", versionNum, ", maxKeys:", maxKeys, "delimiter:", delimiter)

	failed := false

	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
	}
	defer sc.DeleteBucket(TEST_BUCKET)

	if err = sc.PutBucketVersioning(TEST_BUCKET, "Enabled"); err != nil {
		t.Fatal("PutBucketVersioning err :", err)
	}

	// All the dir and objects created.
	expectedDirList := []string{}
	expectedDirObjectList := map[string]([]string){}
	expectedObjectList := []string{} // Key without "/".

	// All the versions created, map[versionId]key .
	expectedVersionKeyMap := map[string]string{}
	defer func() {
		// Delete all the versions.
		if failed {
			return
		}
		for versionId, key := range expectedVersionKeyMap {
			if err := sc.DeleteObjectVersion(TEST_BUCKET, key, versionId); err != nil {
				t.Log("Deletion failed!", key, versionId)
			}
		}
		fmt.Println("Deleted all the objects!")
	}()

	// Create dir * objects * versions, + objNum objects
	for i := 0; i < dirNum; i++ {
		dirName := "dir" + strconv.Itoa(i) + "/"
		expectedKeyList := []string{}

		for j := 0; j < dirObjNum; j++ {
			key := dirName + TEST_KEY + strconv.Itoa(j)
			for k := 0; k < dirObjVersionNum; k++ {
				versionId, err := sc.PutObjectVersioning(TEST_BUCKET, key, TEST_VALUE)
				if err != nil {
					t.Fatal("PutBucketVersioning faile for:", TEST_BUCKET, key)
				}
				expectedVersionKeyMap[versionId] = key
			}
			expectedKeyList = append(expectedKeyList, key)
		}

		expectedDirList = append(expectedDirList, dirName)
		sort.Strings(expectedKeyList)
		expectedDirObjectList[dirName] = expectedKeyList
	}

	for i := dirNum; i < (dirNum + objNum); i++ {
		key := "dir" + strconv.Itoa(i)
		for j := 0; j < versionNum; j++ {
			versionId, err := sc.PutObjectVersioning(TEST_BUCKET, key, TEST_VALUE)
			if err != nil {
				t.Fatal("PutBucketVersioning failed for:", TEST_BUCKET, key)
			}
			expectedVersionKeyMap[versionId] = key
		}

		expectedObjectList = append(expectedObjectList, key)
	}

	t.Log("PubObjects Succeeded!")
	fmt.Println("PubObjects Succeeded!")
	sort.Strings(expectedDirList)
	sort.Strings(expectedObjectList)

	// Get result.
	dirList := []string{}
	dirObjectList := map[string]([]string){}
	objectList := []string{}

	// ListObjects in first level, dir0/ to dirN/, and objects with key "dirX".
	for keyMarker, keyList, commonPrefixes, isTruncated := "", &[]string{}, &[]string{}, true; isTruncated; {
		var err error
		keyMarker, keyList, commonPrefixes, isTruncated, err = sc.ListObjectsWithMarker(TEST_BUCKET, keyMarker, "", delimiter, int64(maxKeys))
		if err != nil {
			t.Fatal("ListObjects failed 1 for:", TEST_BUCKET, keyMarker, delimiter, maxKeys)
		}

		dirList = append(dirList, (*commonPrefixes)...)
		objectList = append(objectList, (*keyList)...)
	}

	// ListObjects in dirX
	for _, commonPrefix := range dirList {
		for keyMarker, keyList, commonPrefixes, isTruncated := commonPrefix, &[]string{}, &[]string{}, true; isTruncated; {
			var err error
			keyMarker, keyList, commonPrefixes, isTruncated, err = sc.ListObjectsWithMarker(TEST_BUCKET, keyMarker, commonPrefix, delimiter, int64(maxKeys))
			if err != nil || len(*commonPrefixes) != 0 {
				t.Fatal("ListObjects failed 2 for:", TEST_BUCKET, keyMarker, commonPrefix, err, commonPrefixes)
			}

			dirObjectList[commonPrefix] = append(dirObjectList[commonPrefix], (*keyList)...)
		}
	}

	// Verify ListObjects result.
	if delimiter != "" {
		if !reflect.DeepEqual(expectedDirList, dirList) {
			t.Fatal("DirList not match:", dirList, "expected:", expectedDirList)
		}
		for dirName, expectedKeyList := range expectedDirObjectList {
			keyList, ok := dirObjectList[dirName]
			if !ok {
				t.Fatal("No keyList for dir:", dirName, len(expectedKeyList))
			}
			if !reflect.DeepEqual(expectedKeyList, keyList) {
				t.Fatal("KeyList not equal:", dirName, len(expectedKeyList), len(keyList))
			}
		}
		if !reflect.DeepEqual(expectedObjectList, objectList) {
			t.Fatal("ObjectList not match:", objectList, "expected:", expectedObjectList)
		}
	} else {
		allExpectedObjectList := []string{}
		for _, keyList := range expectedDirObjectList {
			allExpectedObjectList = append(allExpectedObjectList, keyList...)
		}
		allExpectedObjectList = append(allExpectedObjectList, expectedObjectList...)

		if !reflect.DeepEqual(allExpectedObjectList, objectList) {
			t.Fatal("ObjectList not match:", objectList, "expected:", allExpectedObjectList)
		}
	}

	t.Log("ListObjects Succeeded!")

	// ListObjectVersions, Get dir0/ to dirN/, object dirX.
	versionedDirList := []string{}
	versionKeyMap := map[string]string{}
	for keyVersionList, dirList, isTruncated, keyMarker, versionIdMarker := &[][]string{}, &[]string{}, true, "", ""; isTruncated; {
		var err error
		keyVersionList, dirList, isTruncated, keyMarker, versionIdMarker, err = sc.ListObjectVersions(TEST_BUCKET, keyMarker, versionIdMarker, "", delimiter, int64(maxKeys))
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}

		versionedDirList = append(versionedDirList, (*dirList)...)

		// objNum * versionNum, "dirX" files.
		for _, keyVersion := range *keyVersionList {
			key := keyVersion[0]
			versionId := keyVersion[1]
			versionKeyMap[versionId] = key
		}

		// t.Log("versionKeyMap len1:", len(versionKeyMap))
	}

	// ListObjectVersions in dirN/
	for _, commonPrefix := range versionedDirList {
		for keyVersionList, dirList, isTruncated, keyMarker, versionIdMarker := &[][]string{}, &[]string{}, true, commonPrefix, ""; isTruncated; {
			var err error
			keyVersionList, dirList, isTruncated, keyMarker, versionIdMarker, err = sc.ListObjectVersions(TEST_BUCKET, keyMarker, versionIdMarker, commonPrefix, delimiter, int64(maxKeys))
			if err != nil || len(*dirList) != 0 {
				t.Fatal("ListObjectVersions failed for:", TEST_BUCKET, keyMarker, versionIdMarker, commonPrefix, delimiter, maxKeys)
			}

			// dir[i]/testput[j]
			for _, keyVersion := range *keyVersionList {
				key := keyVersion[0]
				versionId := keyVersion[1]
				versionKeyMap[versionId] = key
			}

			// t.Log("versionKeyMap", commonPrefix, keyMarker, "len:", len(versionKeyMap))
		}
		// t.Log("versionKeyMap", commonPrefix, "len:", len(versionKeyMap))
	}

	// Verify ListObjectVersions.
	if delimiter != "" {
		if !reflect.DeepEqual(expectedDirList, versionedDirList) {
			t.Fatal("ListObjectVersions returned dir not match!")
		}
	}

	if !reflect.DeepEqual(expectedVersionKeyMap, versionKeyMap) {
		failed = true
		if len(versionKeyMap) != len(expectedVersionKeyMap) {
			t.Log("versionKeyMap len not match!", len(versionKeyMap), len(expectedVersionKeyMap))
		}
		t.Fatal("versionKeyMap not match!")
	}

	t.Log("ListObjectVersions with Delimiter Succeeded!")
	fmt.Println("ListObjectVersions with Delimiter Succeeded!")
}

/*
func Test_ListObjectVersions10000(t *testing.T) {

	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer sc.DeleteBucket(TEST_BUCKET)

	if err = sc.PutBucketVersioning(TEST_BUCKET, "Enabled"); err != nil {
		t.Fatal("PutBucketVersioning err :", err)
		panic(err)
	}

	t.Log("Before objects created:", time.Now())

	for i := 0; i < 10000; i++ {
		_, err := sc.PutObjectVersioning(TEST_BUCKET, TEST_KEY, TEST_VALUE)
		if err != nil {
			t.Fatal("PutObjectVersioning failed for", i, err)
		}
	}

	t.Log("All the 10000 objects created:", time.Now())

	keyMarker := ""
	versionIdMarker := ""
	maxKeys := int64(-1)
	var versionCount int
	for {
		var result *[][]string
		var isTruncated bool

		result, isTruncated, keyMarker, versionIdMarker, err = sc.ListObjectVersions(TEST_BUCKET, keyMarker, versionIdMarker, maxKeys)
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}

		if len(*result) != 1000 {
			t.Fatal("ListObjectVersions unexpected result len:", len(*result))
			panic("")
		}

		versionCount += len(*result)
		t.Log("ListObjectVersions: ", versionCount, time.Now())

		if !isTruncated {
			break
		}
	}

	t.Log("After ListObjectVersions:", time.Now())

	if versionCount != 10000 {
		t.Fatal("ListObjectVersions10000 unexpected count:", versionCount)
		panic("")
	}

	if _, err = sc.ListObjects(TEST_BUCKET); err != nil {
		t.Fatal("ListObjects err:", err)
		panic(err)
	}

	t.Log("After ListObjects:", time.Now())

	for {
		var result *[][]string
		var isTruncated bool

		var err error

		result, isTruncated, _, _, err = sc.ListObjectVersions(TEST_BUCKET, "", "", int64(-1))
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}

		for _, keyVersion := range *result {
			_ = sc.DeleteObjectVersion(TEST_BUCKET, TEST_KEY, keyVersion[1])
		}

		if !isTruncated {
			break
		}
	}

	t.Log("After DeleteObjects:", time.Now())

	t.Log("ListObjectVersions 10000 Success")
}
*/
