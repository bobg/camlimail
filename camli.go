package camlimail

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/bobg/rmime"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/schema"
)

// CamPutMsg adds a message to the Perkeep server at dst. The message
// is added as a hierarchy of blobs with the root blob a schema blob
// having camliType "mime-message". See CamPutPart for other details
// of the root schema blob.
func CamPutMsg(dst blobserver.StatReceiver, msg *rmime.Message) (blob.Ref, error) {
	return camPut(dst, (*rmime.Part)(msg), "mime-message")
}

// CamPutMsg adds a message part to the Perkeep server at dst. The
// message part is added as a hierarchy of blobs with the root blob a
// schema blob having camliType "mime-part".
//
// Other fields of the root schema blob:
//   fields: header of the part as a list of (name, list-of-values) pairs
//   content_type: canonicalized content-type of the part
//
// and optionally:
//   time: parsed date of the part
//   charset: charset for text/* parts
//   subject: decoded subject text for message parts
//
// Additionally, the body of the part appears as follows:
//   - for multipart/* parts, as the field "subparts",
//     a list of nested "mime-part" schema blobs
//   - for message/* parts, as the field "submessage",
//     a nested "mime-message" schema blob
//   - for other parts, as the field "body", a reference to a "bytes" schema blob.
func CamPutPart(dst blobserver.StatReceiver, p *rmime.Part) (blob.Ref, error) {
	return camPut(dst, p, "mime-part")
}

func camPut(dst blobserver.StatReceiver, p *rmime.Part, camType string) (blob.Ref, error) {
	var (
		bodyName string
		body     interface{}
	)

	switch p.MajorType() {
	case "multipart":
		multi := p.B.(*rmime.Multipart)
		var subpartRefs []blob.Ref
		for _, subpart := range multi.Parts {
			subpartRef, err := CamPutPart(dst, subpart)
			if err != nil {
				return blob.Ref{}, err
			}
			subpartRefs = append(subpartRefs, subpartRef)
		}
		bodyName = "subparts"
		body = subpartRefs
		// TODO: preamble and postamble?

	case "message":
		submsg := p.B.(*rmime.Message)
		bodyRef, err := CamPutMsg(dst, submsg)
		if err != nil {
			return blob.Ref{}, err
		}
		bodyName = "submessage"
		body = bodyRef

	default:
		bodyR, err := p.Body()
		if err != nil {
			return blob.Ref{}, err
		}
		builder := schema.NewBuilder()
		builder.SetType("bytes")
		bodyRef, err := schema.WriteFileMap(dst, builder, bodyR)
		if err != nil {
			return blob.Ref{}, err
		}
		bodyName = "body"
		body = bodyRef
	}

	m := map[string]interface{}{
		"camliType":    camType,
		"fields":       p.Fields,
		"content_type": p.Type(),
		bodyName:       body,
	}
	if t := p.Time(); t != (time.Time{}) {
		m["time"] = t
	}
	if p.MajorType() == "text" {
		m["charset"] = p.Charset()
	}
	if subj := p.Subject(); subj != "" {
		m["subject"] = subj
	}

	jBytes, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return blob.Ref{}, err
	}

	// Canonical form, according to mapJSON() in
	// perkeep.org/pkg/schema/schema.go.
	jStr := "{\"camliVersion\": 1,\n" + string(jBytes[2:])
	partRef := blob.SHA1FromString(jStr)

	_, err = blobserver.ReceiveNoHash(dst, partRef, strings.NewReader(jStr))
	return partRef, err
}
