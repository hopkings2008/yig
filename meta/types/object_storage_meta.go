package types

import "encoding/json"

type ObjStoreInfo struct {
	// the storage type
	// 0 for ceph stripe
	// 1 for yig stripe
	Type int `json:"type"`
	// One object size in stripe group.
	StripeObjectSize int `json:"objectSize"`
	// stripe unit size
	StripeUnit int `json:"stripeUnit"`
	// number of stripe units in each stripe.
	StripeNum int `json:"stripeNum"`
}

func (osi ObjStoreInfo) Encode() (string, error) {
	buf, err := json.Marshal(&osi)
	return string(buf), err
}

func (osi ObjStoreInfo) Decode(info string) error {
	err := json.Unmarshal([]byte(info), &osi)
	return err
}
