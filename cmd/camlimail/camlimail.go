package main

import (
	"camlimail"
	"flag"
	"log"
	"rmime"
	"strings"

	"github.com/bobg/folder"
	"github.com/bobg/folder/maildir"
	"github.com/bobg/folder/mbox"
	"github.com/bobg/uncompress"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/osutil"
)

func main() {
	server := flag.String("server", "localhost:3179", "camlistore server address")
	roots := flag.String("roots", "", "root blobrefs")
	osutil.AddSecretRingFlag() // xxx it is messed up that this is needed

	flag.Parse()

	var permanodes []blob.Ref
	for _, arg := range strings.Fields(*roots) {
		permanodes = append(permanodes, blob.MustParse(arg))
	}

	client := client.New(*server)

	for _, arg := range flag.Args() {
		f, err := getFolder(arg)
		if err != nil {
			log.Printf("processing %s: %s", arg, err)
			continue
		}
		// xxx add permanode for the folder
		// xxx add folder permanode to root permanodes
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
			// xxx add message ref to folder permanode
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
