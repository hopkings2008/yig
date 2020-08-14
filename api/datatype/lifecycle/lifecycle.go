/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lifecycle

import (
	"encoding/xml"
	"io"

	. "github.com/journeymidnight/yig/error"
)

// DON'T USE IT FOR NEW LC. Old LC configuration. Deprecated. Keep it here to parse the old config in DB.
type LcRule struct {
	ID         string `xml:"ID"`
	Prefix     string `xml:"Prefix"`
	Status     string `xml:"Status"`
	Expiration string `xml:"Expiration>Days"`
}

// DON'T USE IT FOR NEW LC. Old LC configuration. Deprecated. Keep it here to parse the old config in DB.
type Lc struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rule    []LcRule `xml:"Rule"`
}

const (
	// Lifecycle config can't have more than 100 rules
	RulesNumber = 100
)

// Action represents a delete action or other transition
// actions that will be implemented later.
type Action int

const (
	// NoneAction means no action required after evaluating lifecycle rules
	NoneAction Action = iota
	// DeleteAction means the object needs to be removed after evaluating lifecycle rules
	DeleteAction
	// DeleteMarker means the object deleteMarker needs to be removed after evaluating lifecycle rules
	DeleteMarkerAction
	//TransitionAction means the object storage class needs to be transitioned after evaluating lifecycle rules
	TransitionAction
	// AbortMultipartUploadAction means that abort incomplete multipart upload and delete all parts
	AbortMultipartUploadAction
)

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// IsEmpty - returns whether policy is empty or not.
func (lc Lifecycle) IsEmpty() bool {
	return len(lc.Rules) == 0
}

// ParseLifecycleConfig - parses data in given reader to Lifecycle.
func ParseLifecycleConfig(reader io.Reader) (*Lifecycle, error) {
	var lc Lifecycle
	if err := xml.NewDecoder(reader).Decode(&lc); err != nil {
		return nil, err
	}
	if err := lc.Validate(); err != nil {
		return nil, err
	}
	return &lc, nil
}

// Validate - validates the lifecycle configuration
func (lc Lifecycle) Validate() error {
	if len(lc.Rules) > RulesNumber {
		return ErrInvalidLcRulesNumbers
	}
	// Lifecycle config should have at least one rule
	if len(lc.Rules) == 0 {
		return ErrInvalidLcRulesNumbers
	}
	// Validate all the rules in the lifecycle config
	for _, r := range lc.Rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	return nil
}
