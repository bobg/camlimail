package main

import (
	"camlimail"
	"flag"
	"log"
	"strings"

	"github.com/bobg/folder"
	"github.com/bobg/folder/maildir"
	"github.com/bobg/folder/mbox"
	"github.com/bobg/rmime"
	"github.com/bobg/uncompress"
	"perkeep.org/pkg/blob"
	clientpkg "perkeep.org/pkg/client"
	"perkeep.org/pkg/osutil"
	"perkeep.org/pkg/schema"
)

func main() {
	server := flag.String("server", "localhost:3179", "camlistore server address")
	osutil.AddSecretRingFlag() // xxx it is messed up that this is needed

	flag.Parse()

	client := clientpkg.New(*server)

	foldersPermanode, err := permanodeRef(client, "camlimail-folders")
	if err != nil {
		log.Fatalf("getting/creating camlimail-folders permanode: %s", err)
	}
	messagesPermanode, err := permanodeRef(client, "camlimail-messages")
	if err != nil {
		log.Fatalf("getting/creating camlimail-messages permanode: %s", err)
	}

	for _, arg := range flag.Args() {
		f, err := getFolder(arg)
		if err != nil {
			log.Printf("processing %s: %s", arg, err)
			continue
		}
		folderPermanode, err := permanodeRef(client, arg)
		if err != nil {
			log.Printf("getting/creating permanode for folder %s: %s", arg, err)
			continue
		}
		err = addMember(client, foldersPermanode, folderPermanode)
		if err != nil {
			log.Printf("adding permanode for folder %s to camlimail-folders: %s", arg, err)
			continue
		}
		for i := 1; ; i++ {
			msgR, closer, err := f.Message()
			if err != nil {
				log.Fatalf("opening message %d in %s: %s", i, arg, err)
			}
			if msgR == nil {
				break
			}
			msg, err := rmime.ReadMessage(msgR)
			if err != nil {
				log.Fatalf("reading message %d in %s: %s", i, arg, err)
			}
			err = closer()
			if err != nil {
				log.Fatalf("closing message %d in %s: %s", i, arg, err)
			}
			ref, err := camlimail.CamPutMsg(client, msg)
			if err != nil {
				log.Fatalf("adding message %d from %s: %s", i, arg, err)
			}
			log.Printf("message %d in %s added as %s", i, arg, ref)
			err = addMember(client, messagesPermanode, ref)
			if err != nil {
				log.Fatalf("adding message %d from %s to camlimail-messages: %s", i, arg, err)
			}
		}
	}
}

func getFolder(name string) (folder.Folder, error) {
	f, err := maildir.New(name)
	if err == nil {
		return f, nil
	}
	r, err := uncompress.OpenFile(name)
	if err != nil {
		return nil, err
	}
	return mbox.New(r)
}

func permanodeRef(client *clientpkg.Client, key string) (blob.Ref, error) {
	builder := schema.NewPlannedPermanode(key)
	blob := builder.Blob()
	jStr := blob.JSON()
	ref := blob.BlobRef()
	uploadHandle := &clientpkg.UploadHandle{
		BlobRef:  ref,
		Contents: strings.NewReader(jStr),
		Size:     uint32(len(jStr)),
	}
	_, err := client.Upload(uploadHandle)
	return ref, err
}

func addMember(client *clientpkg.Client, dst, src blob.Ref) error {
	builder := schema.NewAddAttributeClaim(dst, "camliMember", src.String())
	blob := builder.Blob()
	jStr := blob.JSON()
	uploadHandle := &clientpkg.UploadHandle{
		BlobRef:  blob.BlobRef(),
		Contents: strings.NewReader(jStr),
		Size:     uint32(len(jStr)),
	}
	_, err := client.Upload(uploadHandle)
	return err
}
