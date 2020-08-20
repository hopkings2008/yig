package _go

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

const (
	TEST_LC_SLEEP_INTERVAL        = 30
	TEST_LC_START_INTERVAL        = 20
	TEST_LC_WAIT_LC_WORK_INTERVAL = 5
)

type LCTestCase struct {
	Name       string
	Versioning bool

	// Versioning Disabled
	CurrentObjectExpiredPerRule         int // Objects matching Prefix and expired.
	CurrentObjectMatchNotExpiredPerRule int // Objects matching Prefix but not expired.
	CurrentObjectNotMatchPrefixPerRule  int // Objects not matching and should not be deleted.

	// Versioning Enabled
	ObjectMatchPerRule              int // Objects matching Prefix.
	ObjectVersionsExpiredPerRule    int // Object versions matching Prefix and expired excluding the last one. Excluding current version.
	ObjectVersionsNotExpiredPerRule int // Object versions matching Prefix but not expired. Excluding current version.
	// And a current verson here not specified.
	ObjectNotMatchPrefixPerRule         int // Object not matching Prefix.
	ObjectVersionsNotMatchPrefixPerRule int // Object versions not matching Prefix. Including current version.

	// Incomplete multipart upload
	MultipartExpiredPerRule         int // Multipart matching Prefix and expired.
	MultipartMatchNotExpiredPerRule int // Multipart match Prefix and not expired.
	MultipartNotMatchPrefixPerRule  int // Multipart not match Prefix

	LifecycleConfiguration *s3.BucketLifecycleConfiguration
}

func Test_LifeCycle_Basic_Rules(t *testing.T) {
	LCTestCases := generateLCTestCases()

	sc := NewS3ForcePathStyle()

	expiredObjectListMap := make(map[string][]string)                   // [bucketName]Key should be deleted.
	notExpiredObjectListMap := make(map[string][]string)                // [bucketName]Key should not be deleted.
	expiredObjectVersionListMap := make(map[string][][]string)          // [bucketName]{Key, VersionId} should be deleted.
	notExpiredObjectVersionListMap := make(map[string][][]string)       // [bucketName]{Key, VersionId} should not be deleted.
	expiredIncompleteMultipartListMap := make(map[string][][]string)    // [bucketName]{Key, UploadId} should be deleted.
	notExpiredIncompleteMultipartListMap := make(map[string][][]string) // [bucketName]{Key, UploadId} should not be deleted.

	var err error

	for _, tc := range LCTestCases {
		bucketName := TEST_BUCKET + "-" + tc.Name

		// Clean up
		defer func() {
			if err = sc.DeleteBucketAllObjectVersions(bucketName); err != nil {
				t.Log("DeleteBucketAllObjects Failed!"+bucketName, err)
			}
			if err = sc.DeleteBucket(bucketName); err != nil {
				t.Log("DeleteBucket Failed! " + bucketName)
			}
		}()

		createBucketAndConfigLC(t, sc, bucketName, tc.LifecycleConfiguration)

		if !tc.Versioning {
			expiredObjectList := []string{}
			notExpiredObjectList := []string{}

			for _, rule := range tc.LifecycleConfiguration.Rules {
				status := aws.StringValue(rule.Status)
				prefix := aws.StringValue(getPrefix(rule))

				// Object match prefix.
				for i := 0; i < tc.CurrentObjectExpiredPerRule; i++ {
					key := prefix + strconv.Itoa(i)
					err = sc.PutObject(bucketName, key, TEST_VALUE)
					if err != nil {
						t.Fatal("PutObject err:", err, bucketName, key)
					}

					if status == "Enabled" {
						expiredObjectList = append(expiredObjectList, key)
					} else {
						notExpiredObjectList = append(notExpiredObjectList, key)
					}
				}

				// Object not match prefix.
				for i := 0; i < tc.CurrentObjectNotMatchPrefixPerRule; i++ {
					key := "_" + prefix + strconv.Itoa(i)
					err = sc.PutObject(bucketName, key, TEST_VALUE)
					if err != nil {
						t.Fatal("PutObject err:", err, bucketName, key)
					}

					notExpiredObjectList = append(notExpiredObjectList, key)
				}
			}

			expiredObjectListMap[bucketName] = expiredObjectList
			notExpiredObjectListMap[bucketName] = notExpiredObjectList
		} else {
			// Versioning Enabled.
			if err = sc.PutBucketVersioning(bucketName, "Enabled"); err != nil {
				t.Fatal("PutBucketVersioning failed!" + bucketName)
			}
			expiredObjectVersionList := [][]string{}
			notExpiredObjectVersionList := [][]string{}

			for _, rule := range tc.LifecycleConfiguration.Rules {
				status := aws.StringValue(rule.Status)
				prefix := aws.StringValue(getPrefix(rule))

				// Object match prefix.
				for i := 0; i < tc.ObjectMatchPerRule; i++ {
					key := prefix + strconv.Itoa(i)

					for j := 0; j < tc.ObjectVersionsExpiredPerRule; j++ {
						var versionId string
						if versionId, err = sc.PutObjectVersioning(bucketName, key, TEST_VALUE); err != nil {
							t.Fatal("PutObjectVersioning err:", err, bucketName, key)
						}
						//t.Log("Created:", bucketName, key, versionId)

						if status == "Enabled" && j != (tc.ObjectVersionsExpiredPerRule-1) {
							expiredObjectVersionList = append(expiredObjectVersionList, []string{key, versionId})
						} else {
							notExpiredObjectVersionList = append(notExpiredObjectVersionList, []string{key, versionId})
						}
					}
				}

				// Object not match prefix.
				for i := 0; i < tc.ObjectNotMatchPrefixPerRule; i++ {
					key := "_" + prefix + strconv.Itoa(i)

					for j := 0; j < tc.ObjectVersionsNotMatchPrefixPerRule; j++ {
						var versionId string
						if versionId, err = sc.PutObjectVersioning(bucketName, key, TEST_VALUE); err != nil {
							t.Fatal("PutObjectVersioning err:", err, bucketName, key)
						}
						notExpiredObjectVersionList = append(notExpiredObjectVersionList, []string{key, versionId})
					}
				}
			}

			expiredObjectVersionListMap[bucketName] = expiredObjectVersionList
			notExpiredObjectVersionListMap[bucketName] = notExpiredObjectVersionList
		}

		// Multipart.
		expiredIncompleteMultipartList := [][]string{}
		notExpiredIncompleteMultipartList := [][]string{}

		for _, rule := range tc.LifecycleConfiguration.Rules {
			status := aws.StringValue(rule.Status)
			prefix := aws.StringValue(getPrefix(rule))

			// Multipart match prefix.
			for i := 0; i < tc.MultipartExpiredPerRule; i++ {
				key := prefix + strconv.Itoa(i)
				uploadId := createMultipartUploadIncomplete(t, sc, bucketName, key)
				defer sc.AbortMultiPartUpload(bucketName, key, uploadId)
				// fmt.Println("Multipart match expired:", bucketName, key, uploadId)

				if status == "Enabled" {
					expiredIncompleteMultipartList = append(expiredIncompleteMultipartList, []string{key, uploadId})
				} else {
					notExpiredIncompleteMultipartList = append(notExpiredIncompleteMultipartList, []string{key, uploadId})
				}
			}
			// Multipart not match prefix.
			for i := 0; i < tc.CurrentObjectNotMatchPrefixPerRule; i++ {
				key := "_" + prefix + strconv.Itoa(i)
				uploadId := createMultipartUploadIncomplete(t, sc, bucketName, key)
				defer sc.AbortMultiPartUpload(bucketName, key, uploadId)

				notExpiredIncompleteMultipartList = append(notExpiredIncompleteMultipartList, []string{key, uploadId})
			}
		}
		expiredIncompleteMultipartListMap[bucketName] = expiredIncompleteMultipartList
		notExpiredIncompleteMultipartListMap[bucketName] = notExpiredIncompleteMultipartList
	}

	// Sleep a while to distinguish from expired objects.
	time.Sleep(TEST_LC_SLEEP_INTERVAL * time.Second)

	// Objects not expired, should not be deleted.
	for _, tc := range LCTestCases {
		bucketName := TEST_BUCKET + "-" + tc.Name

		if !tc.Versioning {
			notExpiredObjectList, ok := notExpiredObjectListMap[bucketName]
			if !ok {
				t.Fatal("Not in notExpiredObjectListMap!", bucketName)
			}

			for _, rule := range tc.LifecycleConfiguration.Rules {
				prefix := aws.StringValue(getPrefix(rule))

				// Objects match prefix, but should not expired this time.
				// Start after index of expired objects in this bucket.
				for i := tc.CurrentObjectExpiredPerRule; i < (tc.CurrentObjectExpiredPerRule + tc.CurrentObjectMatchNotExpiredPerRule); i++ {
					key := prefix + strconv.Itoa(i)
					if err = sc.PutObject(bucketName, key, TEST_VALUE); err != nil {
						t.Fatal("PutObject err:", err, bucketName, key)
					}

					notExpiredObjectList = append(notExpiredObjectList, key)
				}
			}
			notExpiredObjectListMap[bucketName] = notExpiredObjectList
		} else {
			notExpiredObjectVersionList, ok := notExpiredObjectVersionListMap[bucketName]
			if !ok {
				t.Fatal("Not in notExpiredObjectVersionListMap!", bucketName)
			}

			for _, rule := range tc.LifecycleConfiguration.Rules {
				prefix := aws.StringValue(getPrefix(rule))

				for i := 0; i < tc.ObjectMatchPerRule; i++ {
					key := prefix + strconv.Itoa(i)
					for j := 0; j < tc.ObjectVersionsNotExpiredPerRule; j++ {
						var versionId string
						if versionId, err = sc.PutObjectVersioning(bucketName, key, TEST_VALUE); err != nil {
							t.Fatal("PutObjectVersioning err:", err, bucketName, key)
						}
						//t.Log("Created:", bucketName, key, versionId)
						notExpiredObjectVersionList = append(notExpiredObjectVersionList, []string{key, versionId})
					}
				}
			}
			notExpiredObjectVersionListMap[bucketName] = notExpiredObjectVersionList
		}

		// Multipart should not expired.
		notExpiredIncompleteMultipartList := notExpiredIncompleteMultipartListMap[bucketName]
		for _, rule := range tc.LifecycleConfiguration.Rules {
			prefix := aws.StringValue(getPrefix(rule))

			// Multipart match prefix.
			for i := 0; i < tc.MultipartMatchNotExpiredPerRule; i++ {
				key := prefix + strconv.Itoa(i)
				uploadId := createMultipartUploadIncomplete(t, sc, bucketName, key)
				defer sc.AbortMultiPartUpload(bucketName, key, uploadId)

				// fmt.Println("Multipart match not expired:", bucketName, key, uploadId)

				notExpiredIncompleteMultipartList = append(notExpiredIncompleteMultipartList, []string{key, uploadId})
			}
		}
		notExpiredIncompleteMultipartListMap[bucketName] = notExpiredIncompleteMultipartList
	}

	// Get object before lc.
	assertObjectExist(t, &expiredObjectListMap, &expiredObjectVersionListMap, sc)
	assertObjectExist(t, &notExpiredObjectListMap, &notExpiredObjectVersionListMap, sc)
	assertMultipartExist(t, &expiredIncompleteMultipartListMap, sc)
	assertMultipartExist(t, &notExpiredIncompleteMultipartListMap, sc)

	t.Log("GetObject before lc Success! Waiting for start LC.")
	fmt.Println("GetObject before lc Success! Waiting for start LC.")

	// Test "lc.go".
	startLC(t, 0)

	assertObjectNotExist(t, &expiredObjectListMap, &expiredObjectVersionListMap, sc)
	assertObjectExist(t, &notExpiredObjectListMap, &notExpiredObjectVersionListMap, sc)
	assertMultipartNotExist(t, &expiredIncompleteMultipartListMap, sc)
	assertMultipartExist(t, &notExpiredIncompleteMultipartListMap, sc)

	//DeleteBucketLifecycle:Deletes the lifecycle configuration from the bucket.
	for _, tc := range LCTestCases {
		bucketName := TEST_BUCKET + "-" + tc.Name
		_, err = sc.Client.DeleteBucketLifecycle(
			&s3.DeleteBucketLifecycleInput{
				Bucket: aws.String(bucketName),
			})
		if err != nil {
			t.Fatal("DeleteBucketLifecycle err:", err, bucketName)
		}
	}

	t.Log("DeleteBucketLifecycle Success!")
}

func generateLCTestCases() (TCList []LCTestCase) {
	// Versioning Disabled.
	// TC1: 1 enabled Rule. Should delete matching 2 objects.
	TCList = append(TCList, LCTestCase{
		"1rulenoversioning",
		false,   // Versioning Disabled.
		2, 0, 0, // 2 expired, 0 match but not expired, 0 not match.
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("1"),
					},
					ID:     aws.String("1RuleNoVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	// Use deprecated Prefix, rather than Filter>Prefix in this case.
	TCList = append(TCList, LCTestCase{
		"1rulenoversioningprefix",
		false,   // Versioning Disabled.
		2, 0, 0, // 2 expired, 0 match but not expired, 0 not match.
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(10),
					},
					Prefix: aws.String("1"),
					ID:     aws.String("1RuleNoVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC2: 1 enabled Rule. Should delete matching 2 objects, and leave no matching 3 objects.
	TCList = append(TCList, LCTestCase{
		"2rulenoversioning",
		false,   // Versioning Disabled.
		2, 0, 3, // 2 expired, 0 match but not expired, 3 not match.
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("1"),
					},
					ID:     aws.String("1RuleNoVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"2rulenoversioningprefix",
		false,   // Versioning Disabled.
		2, 0, 3, // 2 expired, 0 match but not expired, 3 not match.
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(10),
					},
					Prefix: aws.String("1"),
					ID:     aws.String("1RuleNoVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC3: 1 default Rule. Should delete 3 objects, and leave 2 not expired object.
	TCList = append(TCList, LCTestCase{
		"3defaultrulenoversioning",
		false,
		3, 2, 0,
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("1DefaultRuleNoVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"3defaultrulenoversioningprefix",
		false,
		3, 2, 0,
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Prefix: aws.String(""),
					ID:     aws.String("1DefaultRuleNoVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC4: 2 rule + 2 disabled rule.
	TCList = append(TCList, LCTestCase{
		"4rulenoversioning",
		false,
		3, 2, 3,
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			// Enabled Rules.
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("aaa"),
					},
					ID:     aws.String("4RuleNoVersioning1"),
					Status: aws.String("Enabled"),
				},
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("bbb"),
					},
					ID:     aws.String("4RuleNoVersioning2"),
					Status: aws.String("Enabled"),
				},
				// Disabled rules.
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("ccc"),
					},
					ID:     aws.String("4RuleNoVersioning3"),
					Status: aws.String("Disabled"),
				},
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("ddd"),
					},
					ID:     aws.String("4RuleNoVersioning4"),
					Status: aws.String("Disabled"),
				},
			},
		},
	})

	// TC5: 2 rule + 1 default rule
	TCList = append(TCList, LCTestCase{
		"5rulenoversioning",
		false,
		3, 2, 0,
		0, 0, 0, 0, 0,
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("aaa"),
					},
					ID:     aws.String("3RuleNoVersioning1"),
					Status: aws.String("Enabled"),
				},
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("bbb"),
					},
					ID:     aws.String("3RuleNoVersioning2"),
					Status: aws.String("Enabled"),
				},
				// Default Rule.
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("3RuleNoVersioningDefault"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// Versioning Enabled.
	// TC6.1: 1 enabled Rule. Should delete matching 1 object * 1 version, and leave 1 objects current version.
	TCList = append(TCList, LCTestCase{
		"6-one-enabled-versioning",
		true, // Versioning Enabled.
		0, 0, 0,
		1, 2, 0, 0, 0, // 1 object 1 version expired, 0 match but not expired, 1 current version, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("1"),
					},
					ID:     aws.String("6RuleVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"6-one-enabled-versioning-prefix",
		true, // Versioning Enabled.
		0, 0, 0,
		1, 2, 0, 0, 0, // 1 object 1 version expired, 0 match but not expired, 1 current version, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Prefix: aws.String("1"),
					ID:     aws.String("6RuleVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC6.2: just to verify won't delete current version. Should not delete any object.
	TCList = append(TCList, LCTestCase{
		"6-one-enabled-no-deletion-versioning",
		true, // Versioning Enabled. Should not delete current version.
		0, 0, 0,
		1, 1, 0, 0, 0, // 1 object 0 version expired, 0 match but not expired, 1 current version, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("1"),
					},
					ID:     aws.String("6RuleVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC7: 1 enabled Rule. Should delete matching 3 objects * 3 versions, and leave not expired 1 versions (excluding current verson).
	TCList = append(TCList, LCTestCase{
		"7-one-enabled-versioning",
		true, // Versioning Enabled.
		0, 0, 0,
		3, 3, 1, 0, 0, // 3 object (3 - 1) version expired, 1 version match but not expired, 1 current verson, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("TC7prefix"),
					},
					ID:     aws.String("7TCVersioning"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC8.1: 1 default Rule. Should delete matching 2 objects * (3 - 1) versions, and leave 2 * 1 current verson.
	TCList = append(TCList, LCTestCase{
		"8-default-versioning",
		true, // Versioning Enabled.
		0, 0, 0,
		2, 3, 0, 0, 0, // 2 object (3 - 1) version expired, 0 version match but not expired, 1 current version, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("TC8-1"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC8.2: 1 default Rule. Should delete matching 4 objects * (4 - 1) versions, and leave 2 version , 1 current verson.
	TCList = append(TCList, LCTestCase{
		"8-default-versioning2",
		true, // Versioning Enabled.
		0, 0, 0,
		4, 4, 2, 0, 0, // 4 object 4 version expired, 2 version match but not expired, 1 current version, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("TC8-2"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC9: 1 enabled Rule, 1 disabled default Rule. Should delete matching 4 objects * 4 versions, and leave 2 version , 1 current verson.
	TCList = append(TCList, LCTestCase{
		"9-enable-disable-versioning",
		true, // Versioning Enabled.
		0, 0, 0,
		4, 4, 2, 3, 2, // 4 object (4 - 3) version expired, 2 version match but not expired, 1 current version, 3 * 2 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("TC9prefix"),
					},
					ID:     aws.String("TC9_enabled"),
					Status: aws.String("Enabled"),
				},
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("TC9_disabled"),
					Status: aws.String("Disabled"),
				},
			},
		},
	})
	// TC10: 2 enabled Rule, and 1 default Rule. Should delete matching 4 objects * 4 versions, and leave 2 version , 1 current verson.
	TCList = append(TCList, LCTestCase{
		"10-enable-default-versioning",
		true, // Versioning Enabled.
		0, 0, 0,
		5, 2, 2, 0, 0, // 5 object (2 - 1) version expired, 2 version match but not expired, 1 current version, 0 not match.
		0, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("TC10prefix"),
					},
					ID:     aws.String("TC10_enabled"),
					Status: aws.String("Enabled"),
				},
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("TC10-2prefix"),
					},
					ID:     aws.String("TC10_enabled2"),
					Status: aws.String("Enabled"),
				},
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 20),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("TC10_default"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// Multipart.
	// TC11: 1 enabled Rule. Should delete matching 1 objects.
	TCList = append(TCList, LCTestCase{
		"1rulemultipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		1, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipart"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"1rulemultipart-prefix",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		1, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Prefix: aws.String("multipart"),
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC12: 1 bucket, 3 incomplete multipart. Should expired.
	TCList = append(TCList, LCTestCase{
		"1rule3multipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		3, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipart"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"1rule3multipart-prefix",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		3, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Prefix: aws.String("multipart"),
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC13: 1 bucket, 1 not match prefix multipart, Should not expired.
	TCList = append(TCList, LCTestCase{
		"1rulenotmatchmultipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 1,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipart"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"1rulenotmatchmultipart-prefix",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 1,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Prefix: aws.String("multipart"),
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC14: 1 bucket, 1 default match incomplete multipart. Should expired.
	TCList = append(TCList, LCTestCase{
		"1defaultrulemultipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		1, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})
	TCList = append(TCList, LCTestCase{
		"1defaultrulemultipart-prefix",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		1, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Prefix: aws.String(""),
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC15: 1 bucket, 2 multipart prefix, 1 default multipart. Should all expired.
	TCList = append(TCList, LCTestCase{
		"3rule3multipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		3, 0, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("Amultipart"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUploadA"),
					Status: aws.String("Enabled"),
				},
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("Bmultipart"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUploadB"),
					Status: aws.String("Enabled"),
				},
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUploadDefault"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC16: 1 bucket, 1 multipart expired, 1 multipart not expired.
	TCList = append(TCList, LCTestCase{
		"1rulenotexpiredmultipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		0, 1, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipart"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	// TC17: 1 bucket, 3 prefix, 2 expired, 2 not expired.
	TCList = append(TCList, LCTestCase{
		"1rule3prefix4multipart",
		false, // Versioning Disabled.
		0, 0, 0,
		0, 0, 0, 0, 0,
		2, 2, 0,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipartA"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUploadA"),
					Status: aws.String("Enabled"),
				},
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipartB"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUploadB"),
					Status: aws.String("Enabled"),
				},
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("multipartC"),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUploadC"),
					Status: aws.String("Enabled"),
				},
			},
		},
	})

	return
}

// 2 buckets, Versioning Enabled and Suspended. 3 objects per bucket * ("null" version + 3 versions).
func Test_LifeCycle_Versioning(t *testing.T) {
	sc := NewS3ForcePathStyle()
	var err error

	bucketVersioningEnabled := TEST_BUCKET + "-enabled-lc"
	bucketVersioningSuspended := TEST_BUCKET + "-suspended-lc"
	bucketVersioningDeleteExpiredObjectDeleteMarker := TEST_BUCKET + "-enabled-expire-delete-marker"
	bucketVersioningDeleteExpiredObjectDeleteMarkerDefault := TEST_BUCKET + "-enabled-expire-delete-marker-default"
	bucketList := []string{
		bucketVersioningEnabled,
		bucketVersioningSuspended,
		bucketVersioningDeleteExpiredObjectDeleteMarker,
		bucketVersioningDeleteExpiredObjectDeleteMarkerDefault}
	prefix := "prefix"
	objectNumPerBucket := 3
	versionNumPerObject := 3

	lcConfigMap := make(map[string]*s3.PutBucketLifecycleConfigurationInput)
	lcConfigVersioning := &s3.PutBucketLifecycleConfigurationInput{
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("1"),
					},
					ID:     aws.String("AddDeleteMarkerForCurrent"),
					Status: aws.String("Enabled"),
				},
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("2"),
					},
					ID:     aws.String("DeleteNoncurrentVersions"),
					Status: aws.String("Enabled"),
				},
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(TEST_LC_START_INTERVAL + 25),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("NotExpired"),
					Status: aws.String("Enabled"),
				},
			},
		},
	}
	lcConfigDeleteExpiredObjectDeleteMarker := &s3.PutBucketLifecycleConfigurationInput{
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Expiration: &s3.LifecycleExpiration{
						Days:                      aws.Int64(10),
						ExpiredObjectDeleteMarker: aws.Bool(true),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("1"),
					},
					ID:     aws.String("ExpireObjectDeleteMarker"),
					Status: aws.String("Enabled"),
				},
			},
		},
	}
	lcConfigDeleteExpiredObjectDeleteMarkerDefault := &s3.PutBucketLifecycleConfigurationInput{
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(10),
					},
					Expiration: &s3.LifecycleExpiration{
						Days:                      aws.Int64(10),
						ExpiredObjectDeleteMarker: aws.Bool(true),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("AllExpired"),
					Status: aws.String("Enabled"),
				},
			},
		},
	}
	lcConfigMap[bucketVersioningEnabled] = lcConfigVersioning
	lcConfigMap[bucketVersioningSuspended] = lcConfigVersioning
	lcConfigMap[bucketVersioningDeleteExpiredObjectDeleteMarker] = lcConfigDeleteExpiredObjectDeleteMarker
	lcConfigMap[bucketVersioningDeleteExpiredObjectDeleteMarkerDefault] = lcConfigDeleteExpiredObjectDeleteMarkerDefault

	for _, bucketName := range bucketList {
		deleteBucket := string(bucketName)
		defer func() {
			if err = sc.DeleteBucketAllObjectVersions(deleteBucket); err != nil {
				t.Log("DeleteBucketAllObjects Failed!"+deleteBucket, err)
			}
			if err = sc.DeleteBucket(deleteBucket); err != nil {
				t.Log("DeleteBucket Failed! " + deleteBucket)
			}
		}()

		//Create bucket.
		err := sc.MakeBucket(bucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err, bucketName)
		}
		t.Log("CreateBucket Success! " + bucketName)

		// null version.
		for i := 0; i < objectNumPerBucket; i++ {
			key := strconv.Itoa(i) + prefix
			err = sc.PutObject(bucketName, key, TEST_VALUE)
			if err != nil {
				t.Fatal("PutObject err:", err, bucketName, key)
			}
		}

		// Versioning Enabled.
		if err = sc.PutBucketVersioning(bucketName, "Enabled"); err != nil {
			t.Fatal("PutBucketVersioning failed!" + bucketName)
		}

		// noncurrent and current versions.
		for i := 0; i < objectNumPerBucket; i++ {
			key := strconv.Itoa(i) + prefix
			for j := 0; j < versionNumPerObject; j++ {
				err = sc.PutObject(bucketName, key, TEST_VALUE)
				if err != nil {
					t.Fatal("PutObject err:", err, bucketName, key)
				}
			}
		}
	}

	// Versioning Suspended.
	if err = sc.PutBucketVersioning(bucketVersioningSuspended, "Suspended"); err != nil {
		t.Fatal("PutBucketVersioning failed!"+bucketVersioningSuspended, err)
	}

	// Add delete marker to test expire delete marker.
	for i := 0; i < objectNumPerBucket; i++ {
		key := strconv.Itoa(i) + prefix
		if err = sc.DeleteObject(bucketVersioningDeleteExpiredObjectDeleteMarker, key); err != nil {
			t.Fatal("DeleteObject failed!", err, bucketVersioningDeleteExpiredObjectDeleteMarker, key)
		}
		if err = sc.DeleteObject(bucketVersioningDeleteExpiredObjectDeleteMarkerDefault, key); err != nil {
			t.Fatal("DeleteObject failed!", err, bucketVersioningDeleteExpiredObjectDeleteMarkerDefault, key)
		}
	}

	for _, bucketName := range bucketList {
		lcConfig, ok := lcConfigMap[bucketName]
		if !ok {
			t.Fatal("bucketName not exist!", bucketName)
		}
		lcConfig.Bucket = aws.String(bucketName)
		_, err = sc.Client.PutBucketLifecycleConfiguration(lcConfig)
		if err != nil {
			t.Fatal("PutBucketLifecycle err:", err)
		}
		t.Log("PutBucketLifecycle Success! " + bucketName)
	}

	startLC(t, 0)

	// Check objects after LC.
	// bucketName, key, null, versions except null, delete marker.
	countObjectVersions(t, sc, bucketVersioningEnabled, "1"+prefix, 1, versionNumPerObject, 1) // Add a delete marker.
	countObjectVersions(t, sc, bucketVersioningEnabled, "2"+prefix, 0, 1, 0)                   // Leave a current version.
	countObjectVersions(t, sc, bucketVersioningEnabled, "0"+prefix, 1, versionNumPerObject, 0) // Do nothing.

	countObjectVersions(t, sc, bucketVersioningSuspended, "1"+prefix, 0, versionNumPerObject, 1) // In fact 1 "null" delete marker.
	countObjectVersions(t, sc, bucketVersioningSuspended, "2"+prefix, 0, 1, 0)                   // Leave a current version.
	countObjectVersions(t, sc, bucketVersioningSuspended, "0"+prefix, 1, versionNumPerObject, 0) // Do nothing.

	countObjectVersions(t, sc, bucketVersioningDeleteExpiredObjectDeleteMarker, "1"+prefix, 0, 0, 0)                   // Delete all the versions and delete expired object delete marker.
	countObjectVersions(t, sc, bucketVersioningDeleteExpiredObjectDeleteMarker, "2"+prefix, 1, versionNumPerObject, 1) // Do nothing.
	countObjectVersions(t, sc, bucketVersioningDeleteExpiredObjectDeleteMarker, "0"+prefix, 1, versionNumPerObject, 1) // Do nothing.

	countObjectVersions(t, sc, bucketVersioningDeleteExpiredObjectDeleteMarkerDefault, "1"+prefix, 0, 0, 0) // Delete all the versions and delete expired object delete marker.
	countObjectVersions(t, sc, bucketVersioningDeleteExpiredObjectDeleteMarkerDefault, "2"+prefix, 0, 0, 0) // Delete all the versions and delete expired object delete marker.
	countObjectVersions(t, sc, bucketVersioningDeleteExpiredObjectDeleteMarkerDefault, "0"+prefix, 0, 0, 0) // Delete all the versions and delete expired object delete marker.

	t.Log("Test_LifeCycle_VersioningAddDeleteMarker Succeeded!")
}

func Test_LifeCycle_Abnormal_Config(t *testing.T) {
	sc := NewS3ForcePathStyle()

	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err, TEST_BUCKET)
	}
	defer sc.DeleteBucket(TEST_BUCKET)

	// Should succeed with 100 rules.
	lifecycleConfiguration := s3.BucketLifecycleConfiguration{}
	ruleNum := 100
	for i := 0; i < ruleNum; i++ {
		lifecycleConfiguration.Rules = append(lifecycleConfiguration.Rules,
			&s3.LifecycleRule{
				Expiration: &s3.LifecycleExpiration{
					Days: aws.Int64(10),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(strconv.Itoa(i)),
				},
				ID:     aws.String(strconv.Itoa(i)),
				Status: aws.String("Enabled"),
			})
	}
	_, err = sc.Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(TEST_BUCKET),
		LifecycleConfiguration: &lifecycleConfiguration,
	})
	if err != nil {
		t.Fatal("PutBucketLifecycle 100 rules failed!", TEST_BUCKET, lifecycleConfiguration)
	}

	t.Log("Config lifecycle", len(lifecycleConfiguration.Rules), "succeeded!")

	// Should fail with 100 rules.
	tooLargeRuleNum := ruleNum + 1
	for i := ruleNum; i < tooLargeRuleNum; i++ {
		lifecycleConfiguration.Rules = append(lifecycleConfiguration.Rules,
			&s3.LifecycleRule{
				Expiration: &s3.LifecycleExpiration{
					Days: aws.Int64(10),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(strconv.Itoa(i)),
				},
				ID:     aws.String(strconv.Itoa(i)),
				Status: aws.String("Enabled"),
			})
	}
	_, err = sc.Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(TEST_BUCKET),
		LifecycleConfiguration: &lifecycleConfiguration,
	})
	if err == nil {
		t.Fatal("PutBucketLifecycle", len(lifecycleConfiguration.Rules), "rules failed!", TEST_BUCKET, lifecycleConfiguration)
	}

	// Duplicated Rule ID, should fail.
	lifecycleConfiguration.Rules = lifecycleConfiguration.Rules[:0]
	lifecycleConfiguration.Rules = append(lifecycleConfiguration.Rules, &s3.LifecycleRule{
		ID: aws.String("1"),
		Filter: &s3.LifecycleRuleFilter{
			Prefix: aws.String("1"),
		},
		Status: aws.String("Enabled"),
	})
	lifecycleConfiguration.Rules = append(lifecycleConfiguration.Rules, &s3.LifecycleRule{
		ID: aws.String("1"),
		Filter: &s3.LifecycleRuleFilter{
			Prefix: aws.String("2"),
		},
		Status: aws.String("Enabled"),
	})
	_, err = sc.Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(TEST_BUCKET),
		LifecycleConfiguration: &lifecycleConfiguration,
	})
	if err == nil {
		t.Fatal("PutBucketLifecycle", len(lifecycleConfiguration.Rules), "rules failed!", TEST_BUCKET, lifecycleConfiguration)
	}

	// Duplicated Prefix and Filter>Prefix, should fail.
	lifecycleConfiguration.Rules = lifecycleConfiguration.Rules[:0]
	lifecycleConfiguration.Rules = append(lifecycleConfiguration.Rules, &s3.LifecycleRule{
		ID: aws.String("1"),
		Filter: &s3.LifecycleRuleFilter{
			Prefix: aws.String("1"),
		},
		Prefix: aws.String("1"),
		Status: aws.String("Enabled"),
	})
	_, err = sc.Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(TEST_BUCKET),
		LifecycleConfiguration: &lifecycleConfiguration,
	})
	if err == nil {
		t.Fatal("PutBucketLifecycle", len(lifecycleConfiguration.Rules), "rules failed!", TEST_BUCKET, lifecycleConfiguration)
	}

	t.Log("Config lifecycle", len(lifecycleConfiguration.Rules), "rejected as expected!", err)
}

/*
// Test scan bucket in lc.go.
func Test_LifeCycle_200_Buckets(t *testing.T) {
	sc := NewS3ForcePathStyle()
	var err error

	testBucketNum := 200
	prefix := "prefix"
	objectNumPerBucket := 2
	versionNumPerObject := 2

	// Versioning Disabled / Enabled / Suspended.
	for i := 0; i < testBucketNum; i++ {
		// time.Sleep(time.Second * 1)
		bucketName := TEST_BUCKET + strconv.Itoa(i)
		defer cleanup(t, sc, bucketName)

		//Create bucket.
		err = sc.MakeBucket(bucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err, bucketName)
		}
		// t.Log("CreateBucket Success! " + bucketName)

		_, err = sc.Client.PutBucketLifecycleConfiguration(
			&s3.PutBucketLifecycleConfigurationInput{
				Bucket: aws.String(bucketName),
				LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
					Rules: []*s3.LifecycleRule{
						{
							Expiration: &s3.LifecycleExpiration{
								Days: aws.Int64(1),
							},
							Filter: &s3.LifecycleRuleFilter{
								Prefix: aws.String("0"),
							},
							ID:     aws.String("AddDeleteMarkerForCurrent"),
							Status: aws.String("Enabled"),
						},
						{
							NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
								NoncurrentDays: aws.Int64(1),
							},
							Filter: &s3.LifecycleRuleFilter{
								Prefix: aws.String("1"),
							},
							ID:     aws.String("DeleteNoncurrentVersions"),
							Status: aws.String("Enabled"),
						},
					},
				},
			})
		if err != nil {
			t.Fatal("PutBucketLifecycle err:", err)
		}

		// null version.
		for i := 0; i < objectNumPerBucket; i++ {
			key := strconv.Itoa(i) + prefix
			err = sc.PutObject(bucketName, key, TEST_VALUE)
			if err != nil {
				t.Fatal("PutObject err:", err, bucketName, key)
			}
		}

		if isVersioningDisabled(i) {
			continue
		}

		// Versioning Enabled.
		if err = sc.PutBucketVersioning(bucketName, "Enabled"); err != nil {
			t.Fatal("PutBucketVersioning failed!" + bucketName)
		}

		// noncurrent and current versions.
		for i := 0; i < objectNumPerBucket; i++ {
			key := strconv.Itoa(i) + prefix
			for j := 0; j < versionNumPerObject; j++ {
				err = sc.PutObject(bucketName, key, TEST_VALUE)
				if err != nil {
					t.Fatal("PutObject err:", err, bucketName, key)
				}
			}
		}

		if isVersioningEnabled(i) {
			continue
		}

		if err = sc.PutBucketVersioning(bucketName, "Suspended"); err != nil {
			t.Fatal("PutBucketVersioning failed!"+bucketName, err)
		}
	}

	startLC(t, 60)

	// Verify.
	for i := 0; i < testBucketNum; i++ {
		bucketName := TEST_BUCKET + strconv.Itoa(i)
		if isVersioningDisabled(i) {
			countObjectVersions(t, sc, bucketName, "0"+prefix, 0, 0, 0) // deleted.
			countObjectVersions(t, sc, bucketName, "1"+prefix, 1, 0, 0) // Only one current null version. Not deleted.
		} else if isVersioningEnabled(i) {
			countObjectVersions(t, sc, bucketName, "0"+prefix, 1, versionNumPerObject, 1) // Add a delete marker.
			countObjectVersions(t, sc, bucketName, "1"+prefix, 0, 1, 0)                   // Leave only current version.
		} else {
			countObjectVersions(t, sc, bucketName, "0"+prefix, 0, versionNumPerObject, 1) // deleted.
			countObjectVersions(t, sc, bucketName, "1"+prefix, 0, 1, 0)                   // Only one current null version. Not deleted.
		}
	}
}

// Delete objects key start with "1" from "0" to "3999" objects.
func Test_LifeCycle_4000_Objects(t *testing.T) {
	bucketName := TEST_BUCKET
	objectNumPerBucket := 4000
	prefix := "prefix"
	lcFilterPrefix := "1"
	var err error

	sc := NewS3ForcePathStyle()

	defer cleanup(t, sc, bucketName)

	//Create bucket.
	err = sc.MakeBucket(bucketName)
	if err != nil {
		t.Fatal("MakeBucket err:", err, bucketName)
	}
	t.Log("CreateBucket Success! " + bucketName)

	_, err = sc.Client.PutBucketLifecycleConfiguration(
		&s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucketName),
			LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
				Rules: []*s3.LifecycleRule{
					{
						Expiration: &s3.LifecycleExpiration{
							Days: aws.Int64(1),
						},
						Filter: &s3.LifecycleRuleFilter{
							Prefix: aws.String(lcFilterPrefix),
						},
						ID:     aws.String("TestListObjects"),
						Status: aws.String("Enabled"),
					},
				},
			},
		})
	if err != nil {
		t.Fatal("PutBucketLifecycle err:", err)
	}

	expiredObjectList := []string{}
	notExpiredObjectList := []string{}

	// null version.
	for i := 0; i < objectNumPerBucket; i++ {
		key := strconv.Itoa(i) + prefix
		err = sc.PutObject(bucketName, key, TEST_VALUE)
		if err != nil {
			t.Fatal("PutObject err:", err, bucketName, key)
		}

		if strings.HasPrefix(key, lcFilterPrefix) {
			expiredObjectList = append(expiredObjectList, key)
		} else {
			notExpiredObjectList = append(notExpiredObjectList, key)
		}
	}

	startLC(t, 120)

	// Verify
	expiredBucketMap := make(map[string][]string)
	expiredBucketMap[bucketName] = expiredObjectList
	notExpiredBucketMap := make(map[string][]string)
	notExpiredBucketMap[bucketName] = notExpiredObjectList

	assertObjectNotExist(t, &expiredBucketMap, nil, sc)
	assertObjectExist(t, &notExpiredBucketMap, nil, sc)
}

func Test_LifeCycle_4000_Object_Versions(t *testing.T) {
	bucketName := TEST_BUCKET
	objectVersionsPerBucket := 4000
	prefix := "prefix"
	var err error

	sc := NewS3ForcePathStyle()

	defer cleanup(t, sc, bucketName)

	//Create bucket.
	err = sc.MakeBucket(bucketName)
	if err != nil {
		t.Fatal("MakeBucket err:", err, bucketName)
	}
	t.Log("CreateBucket Success! " + bucketName)

	// Versioning Enabled.
	if err = sc.PutBucketVersioning(bucketName, "Enabled"); err != nil {
		t.Fatal("PutBucketVersioning failed!" + bucketName)
	}

	_, err = sc.Client.PutBucketLifecycleConfiguration(
		&s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucketName),
			LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
				Rules: []*s3.LifecycleRule{
					{
						NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
							NoncurrentDays: aws.Int64(10),
						},
						Filter: &s3.LifecycleRuleFilter{
							Prefix: aws.String(prefix),
						},
						ID:     aws.String("TestListObjectVersions"),
						Status: aws.String("Enabled"),
					},
				},
			},
		})
	if err != nil {
		t.Fatal("PutBucketLifecycle err:", err)
	}

	for i := 0; i < objectVersionsPerBucket; i++ {
		err = sc.PutObject(bucketName, prefix, TEST_VALUE)
		if err != nil {
			t.Fatal("PutObject err:", err, bucketName, prefix)
		}
	}

	startLC(t, 360)

	countObjectVersions(t, sc, bucketName, prefix, 0, 1, 0)
}

func Test_LifeCycle_Abort4000IncompleteMultipart(t *testing.T) {
	prefix := TEST_KEY + "_multipart"
	multipartNum := 4000
	var err error

	sc := NewS3ForcePathStyle()

	bucketName := TEST_BUCKET
	defer cleanup(t, sc, bucketName)
	createBucketAndConfigLC(t, sc, bucketName,
		&s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(10),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(prefix),
					},
					ID:     aws.String("TestAbortIncompleteMultipartUpload"),
					Status: aws.String("Enabled"),
				},
			},
		})

	_, err = sc.Client.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Fatal("GetBucketLifecycleConfiguration failed!", bucketName)
	}

	// Create multipart and don't complete.
	for j := 0; j <= multipartNum; j++ {
		key := prefix
		uploadId := createMultipartUploadIncomplete(t, sc, bucketName, key)
		defer sc.AbortMultiPartUpload(bucketName, key, uploadId)
	}

	fmt.Println("All the objcets created!")

	startLC(t, 280)

	// Should all be deleted except for notMatchPrefix.
	result, err := sc.Client.ListMultipartUploads(&s3.ListMultipartUploadsInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Fatal("ListMultipartUploads failed!", bucketName)
	}

	if len(result.Uploads) != 0 || aws.BoolValue(result.IsTruncated) != false {
		t.Fatal("Upload should be deleted!", bucketName, result.Uploads)
	}
}
*/

func cleanup(t *testing.T, sc *S3Client, bucketName string) {
	if err := sc.DeleteBucketAllObjectVersions(bucketName); err != nil {
		t.Log("DeleteBucketAllObjects Failed!"+bucketName, err)
	}
	if err := sc.DeleteBucket(bucketName); err != nil {
		t.Log("DeleteBucket Failed! " + bucketName)
	}
}

func createBucketAndConfigLC(t *testing.T, sc *S3Client, bucketName string, LifecycleConfiguration *s3.BucketLifecycleConfiguration) {
	//Create bucket.
	err := sc.MakeBucket(bucketName)
	if err != nil {
		t.Fatal("MakeBucket err:", err, bucketName)
	}
	// t.Log("CreateBucket Success! " + bucketName)

	_, err = sc.Client.PutBucketLifecycleConfiguration(
		&s3.PutBucketLifecycleConfigurationInput{
			Bucket:                 aws.String(bucketName),
			LifecycleConfiguration: LifecycleConfiguration,
		})
	if err != nil {
		t.Fatal("PutBucketLifecycle err:", err)
	}

	_, err = sc.Client.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Fatal("GetBucketLifecycleConfiguration failed!", bucketName)
	}
}

// Create a multipart upload and
func createMultipartUploadIncomplete(t *testing.T, sc *S3Client, bucketName, key string) (uploadId string) {
	var err error
	if uploadId, err = sc.CreateMultiPartUpload(bucketName, key, s3.ObjectStorageClassStandard); err != nil {
		t.Fatal("CreateMultiPartUpload err:", err, bucketName, key)
	}

	return
}

func startLC(t *testing.T, waitTimeInSecond int) {
	t.Log("Waiting for lc start.")
	fmt.Println("Waiting for lc start.")

	if waitTimeInSecond <= 0 {
		waitTimeInSecond = TEST_LC_WAIT_LC_WORK_INTERVAL
	}

	// Test "lc.go".
	err := os.Chdir("../../integrate")
	if err != nil {
		t.Fatal("change dir in lc err:", err)
	}

	cmd := exec.Command("bash", "runlc.sh")

	err = cmd.Run()
	if err != nil {
		t.Fatal("lc err:", err)
	}

	t.Log("Start lc Success! Wait for lc finish.")
	fmt.Println("Start lc Success! Wait for lc finish.")
	time.Sleep(time.Second * time.Duration(TEST_LC_START_INTERVAL+waitTimeInSecond))

	cmd = exec.Command("bash", "stoplc.sh")
	if err = cmd.Run(); err != nil {
		t.Fatal("lc stop err:", err)
	}

	fmt.Println("Stopped lc!")
	os.Chdir("../test/go")
}

func assertObjectExist(t *testing.T, bucketKeyMap *map[string][]string, bucketKeyVersionMap *map[string][][]string, sc *S3Client) {
	if bucketKeyMap != nil {
		for bucketName, keyList := range *bucketKeyMap {
			for _, key := range keyList {
				v, err := sc.GetObject(bucketName, key)
				if err != nil {
					t.Fatal("GetObject lc err:", err, bucketName, key)
				}
				if v != TEST_VALUE {
					t.Fatal("GetObject lc err: value is:", v, ", but should be:", TEST_VALUE, "object:", bucketName, key)
				}
			}
		}
	}

	if bucketKeyVersionMap != nil {
		for bucketName, keyVersionList := range *bucketKeyVersionMap {
			for _, keyVersion := range keyVersionList {
				key, versionId := keyVersion[0], keyVersion[1]
				v, err := sc.GetObjectVersion(bucketName, key, versionId)
				if err != nil {
					t.Fatal("GetObject lc err:", err, bucketName, key)
				}
				if v != TEST_VALUE {
					t.Fatal("GetObject lc err: value is:", v, ", but should be:", TEST_VALUE, "object:", bucketName, key, versionId)
				}
			}
		}
	}
}

func assertObjectNotExist(t *testing.T, bucketKeyMap *map[string][]string, bucketKeyVersionMap *map[string][][]string, sc *S3Client) {
	if bucketKeyMap != nil {
		for bucketName, keyList := range *bucketKeyMap {
			for _, key := range keyList {
				_, err := sc.GetObject(bucketName, key)
				if err != nil {
					str := err.Error()
					if !strings.Contains(str, "NoSuchKey: The specified key does not exist") {
						t.Fatal("GetObject after lc test Fail!", err, "object:", bucketName, key)
					}
				} else {
					t.Fatal("GetObject after lc test Fail!", err, "object exist:", bucketName, key)
				}
			}
		}
	}

	if bucketKeyVersionMap != nil {
		for bucketName, keyVersionList := range *bucketKeyVersionMap {
			for _, keyVersion := range keyVersionList {
				key, versionId := keyVersion[0], keyVersion[1]
				_, err := sc.GetObjectVersion(bucketName, key, versionId)
				if err != nil {
					str := err.Error()
					if !strings.Contains(str, "NoSuchKey: The specified key does not exist") {
						t.Fatal("GetObject after lc test Fail!", err, "object:", bucketName, key)
					}
				} else {
					t.Fatal("GetObject after lc test Fail!", err, "object exist:", bucketName, key, versionId)
				}
			}
		}
	}
}

func deleteAllExistObjects(t *testing.T, bucketKeyMap *map[string][]string, bucketKeyVersionMap *map[string][][]string, sc *S3Client) {
	for bucketName, keyList := range *bucketKeyMap {
		for _, key := range keyList {
			if err := sc.DeleteObject(bucketName, key); err != nil {
				t.Log("DeleteObject Failed!", err, bucketName, key)
			} else {
				t.Log("Deleted", bucketName, key)
			}
		}
	}
	for bucketName, keyVersionList := range *bucketKeyVersionMap {
		for _, keyVersion := range keyVersionList {
			key, versionId := keyVersion[0], keyVersion[1]
			if err := sc.DeleteObjectVersion(bucketName, key, versionId); err != nil {
				t.Log("DeleteObjectVersion failed!", err, bucketName, key, versionId)
			} else {
				t.Log("Deleted	", bucketName, key, versionId)
			}
		}
	}
}

func assertMultipartExist(t *testing.T, bucketKeyUploadIdMap *map[string][][]string, sc *S3Client) {
	if bucketKeyUploadIdMap != nil {
		for bucketName, keyUploadIdList := range *bucketKeyUploadIdMap {
			// Get all the incomplete multipart in the bucket.
			// TODO: assume incomplete not exceeding 1000.
			existUploadIdKeyMap := make(map[string]string)
			result, err := sc.Client.ListMultipartUploads(&s3.ListMultipartUploadsInput{Bucket: aws.String(bucketName)})
			if err != nil {
				t.Fatal("ListMultipartUploads failed!", bucketName)
			}
			for _, upload := range result.Uploads {
				existUploadIdKeyMap[aws.StringValue(upload.UploadId)] = aws.StringValue(upload.Key)
			}

			// Compare uploads expected and get.
			for _, keyUploadId := range keyUploadIdList {
				key, uploadId := keyUploadId[0], keyUploadId[1]
				if existKey, ok := existUploadIdKeyMap[uploadId]; !ok || existKey != key {
					t.Fatal("uploadId not exist!", bucketName, key, uploadId, ok, existKey)
				}
			}
		}
	}
}

func assertMultipartNotExist(t *testing.T, bucketKeyUploadIdMap *map[string][][]string, sc *S3Client) {
	if bucketKeyUploadIdMap != nil {
		for bucketName, keyUploadIdList := range *bucketKeyUploadIdMap {
			// Get all the incomplete multipart in the bucket.
			// TODO: assume incomplete not exceeding 1000.
			existUploadIdKeyMap := make(map[string]string)
			result, err := sc.Client.ListMultipartUploads(&s3.ListMultipartUploadsInput{Bucket: aws.String(bucketName)})
			if err != nil {
				t.Fatal("ListMultipartUploads failed!", bucketName)
			}
			for _, upload := range result.Uploads {
				existUploadIdKeyMap[aws.StringValue(upload.UploadId)] = aws.StringValue(upload.Key)
			}

			// Compare uploads expected and get.
			for _, keyUploadId := range keyUploadIdList {
				key, uploadId := keyUploadId[0], keyUploadId[1]
				if existKey, ok := existUploadIdKeyMap[uploadId]; ok {
					t.Fatal("uploadId not exist!", bucketName, key, uploadId, ok, existKey)
				}
			}
		}
	}
}

func isVersioningDisabled(n int) bool {
	return n%3 == 0
}
func isVersioningEnabled(n int) bool {
	return n%3 == 1
}

// versionNum excluding deleteMarker or null version.
// If it's a null delete marker, only count in deleteMarkerNum and won't count in nullNum.
func countObjectVersions(t *testing.T, s3client *S3Client, bucketName, key string, expectedNullNum, expectedVersionNum, expectedDeleteMarkerNum int) {
	result, err := s3client.Client.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(key),
	})
	if err != nil {
		t.Fatal("Failed to ListObjectVersions for", bucketName, key)
	}

	nullCount := 0
	versionCount := 0
	for _, object := range result.Versions {
		if object.VersionId == nil {
			t.Fatal(bucketName, key, "nil VersionId!")
		}
		if *object.VersionId == "null" {
			nullCount++
		} else {
			versionCount++
		}
	}

	deleteMarkerCount := len(result.DeleteMarkers)

	if nullCount != expectedNullNum || versionCount != expectedVersionNum || deleteMarkerCount != expectedDeleteMarkerNum {
		t.Fatal(bucketName, key, "Expected (null, version, deleteMarker):", expectedNullNum, expectedVersionNum, expectedDeleteMarkerNum,
			", List result:", nullCount, versionCount, deleteMarkerCount)
	}

	return
}

func getPrefix(rule *s3.LifecycleRule) *string {
	if rule.Filter != nil && rule.Filter.Prefix != nil && aws.StringValue(rule.Filter.Prefix) != "" {
		return rule.Filter.Prefix
	}
	return rule.Prefix
}
