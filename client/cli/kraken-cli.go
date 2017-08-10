package krakencli

import (
	cfg "code.uber.internal/infra/kraken/config/origin"
	"code.uber.internal/infra/kraken/rendezvous"
	"github.com/spaolacci/murmur3"

	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"sort"
)

//NodeToServer is a node label to origin server lookup index
type nodeToServer map[string]string

//ServerToNode is an origin server to hashing node lookup index
type serverToNode map[string]*weightedhash.RendezvousHashNode

var (
	errOriginIsRequired = errors.New(
		"origin server is a required parameter")
	errDigestIsRequired = errors.New(
		"content's digest is a required parameter")
	errNotImplemented = errors.New(
		"command is not implemented yet")
	errDigestOrOriginIsRequired = errors.New(
		"either content digest or origin server is required")
)

//CommandContent
type commandContext struct {
	writer    io.Writer                    // default stdout writer
	verbose   bool                         // verbosity of boolean
	nts       nodeToServer                 // node to server index
	stn       serverToNode                 // server to node index
	appCfg    cfg.AppConfig                // application config
	hashstate *weightedhash.RendezvousHash // a hashstate configuration parsed from appconfig or defined by other means (rn only config supported)
}

//OriginCapacity represents Capacity and utiulization of a particular origin server that
//endpoint /info returns
type OriginCapacity struct {
	Capacity    int64 `json:"capacity"`    // general origin server capacity in bytes
	Utilization int64 `json:"utilization"` // currently used in bytes
}

//OriginContent represents a single content item on stored on origin server
type OriginContent struct {
	Digest string `json:"digest"` // sha256 content key
	Size   int64  `json:"size"`   // size of a content item in bytes
	Path   string `json:"path"`   // local directory path that has a content item: Path/Digestis a full path
}

//OriginContentList is a list of content items from an origin server
type OriginContentList struct {
	OriginContentItems []OriginContent `json:"items"`
}

func isFlagDefined(commandline []string, flag string) bool {
	for _, f := range commandline {
		if f == flag || f == "-"+flag {
			return true
		}
	}
	return false
}

func sortedHashstateNodes(hashstate map[string]cfg.HashNodeConfig) []string {
	var keys []string
	for _, node := range hashstate {
		keys = append(keys, node.Label)
	}

	sort.Strings(keys)
	return keys
}
func originInfo(originLabel string, cc commandContext) (*OriginCapacity, error) {
	origin, ok := cc.nts[originLabel]
	if !ok {
		err := fmt.Errorf("no such origin server in configuration has been found: %s", originLabel)
		return nil, err
	}

	httpClient := http.Client{}
	resp, err := httpClient.Get("http://" + origin + "/info")
	if err != nil {
		return nil, err
	}

	capacity := &OriginCapacity{}
	err = json.NewDecoder(resp.Body).Decode(capacity)
	if err != nil {
		return nil, err
	}

	return capacity, nil
}

func handleOriginListCommand(originInfoOpt string, listOrigin string, cc commandContext) (int, error) {

	if listOrigin == "" {
		sorted := sortedHashstateNodes(cc.appCfg.Hashstate)
		for _, nl := range sorted {
			originCapacity, err := originInfo(nl, cc)
			if err != nil {
				return 1, err
			}
			//TODO(igor): proper handling for MBs, GBs
			fmt.Fprintf(cc.writer, "%s: [capacity = %d bytes, utilization = %d bytes]\n",
				nl, originCapacity.Capacity, originCapacity.Utilization)
		}
	} else {
		originCapacity, err := originInfo(listOrigin, cc)
		if err != nil {
			return 1, err
		}

		fmt.Fprintf(cc.writer, "%s: [capacity = %d bytes, utilization = %d bytes]\n",
			listOrigin, originCapacity.Capacity, originCapacity.Utilization)
	}
	//TODO(igor): calculate and print total for all origins

	return 0, nil
}

func contentList(originLabel string, cc commandContext) (*OriginContentList, error) {
	origin, ok := cc.nts[originLabel]
	if !ok {
		err := fmt.Errorf("not such origin server in configuration has been found: %s", originLabel)
		return nil, err
	}

	httpClient := http.Client{}
	url := "http://" + origin + "/content/"

	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}

	contentList := &OriginContentList{}
	err = json.NewDecoder(resp.Body).Decode(contentList)
	if err != nil {
		return nil, err
	}

	return contentList, nil
}

func prettyPrintContent(ci *OriginContentList, w io.Writer) {
	for _, item := range ci.OriginContentItems {
		fmt.Fprintf(w, "%s %d %s\n", item.Digest, item.Size, item.Path)
	}
}

func handleContentListCommand(listOption string, listContentOrigin string, cc commandContext) (int, error) {
	if listContentOrigin == "" {
		sorted := sortedHashstateNodes(cc.appCfg.Hashstate)
		for _, nl := range sorted {
			fmt.Fprintf(cc.writer, "Origin %s:\n", nl)
			contentList, err := contentList(nl, cc)
			if err != nil {
				// not returning error here. Want to get through all origin servers
				fmt.Fprintf(cc.writer, err.Error())
			} else {
				prettyPrintContent(contentList, cc.writer)
			}
		}
	} else {
		contentList, err := contentList(listContentOrigin, cc)
		if err != nil {
			return 1, err
		}
		fmt.Fprintf(cc.writer, "Origin %s:\n", listContentOrigin)
		prettyPrintContent(contentList, cc.writer)
	}

	return 0, nil
}

// this will filter out nodes that are not equal to origin label if origin is not empty
// otherwise will return a full list of nodes
func filterOutNodes(nodes []*weightedhash.RendezvousHashNode, origin string, cc commandContext) []*weightedhash.RendezvousHashNode {
	if origin == "" {
		return nodes
	}

	var r []*weightedhash.RendezvousHashNode
	for _, node := range nodes {
		if node.Label == origin {
			r = append(r, node)
		}
	}

	return r
}

func repairContentItemAtOrigin(digest string, originLabel string, cc commandContext) (int, error) {

	var err error
	var resp *http.Response

	origin, ok := cc.nts[originLabel]
	httpClient := http.Client{}

	if !ok {
		err = fmt.Errorf("could not find origin server for %s", originLabel)
		goto done
	}

	resp, err = httpClient.Head("http://" + origin + "/blobs/" + digest)
	if err != nil {
		goto done
	}

	// not found, needs to repair
	if resp.StatusCode == 404 {
		// this is a blocking call and it waits until the content is transferred
		// from other origin replicas
		if resp, err = httpClient.Post("http://"+origin+"/repair/"+digest, "application/json", nil); err != nil {
			goto done
		}
	}

done:
	status := "OK"
	if err != nil {
		status = err.Error()
	} else if resp != nil && resp.StatusCode != 200 {
		status = fmt.Sprintf("%d", resp.StatusCode)
	}
	fmt.Fprintf(cc.writer, "origin: %s, content: %s, repaired: %s",
		originLabel, digest, status)

	return 0, nil

}
func repairContent(digest string, origin string, cc commandContext) (int, error) {
	// get hashstate for a content item
	nodes, err := cc.hashstate.GetOrderedNodes(digest, cc.appCfg.NumReplica)
	if err != nil {
		return 1, err
	}

	nodes = filterOutNodes(nodes, origin, cc)

	// iterate through all origins that may have the content and delete it
	for _, node := range nodes {
		if digest != "" {
			repairContentItemAtOrigin(digest, node.Label, cc)
		} else {
			cl, err := contentList(node.Label, cc)
			if err == nil {
				for _, item := range cl.OriginContentItems {
					repairContentItemAtOrigin(item.Digest, node.Label, cc)
				}
			}
		}

	}

	return 0, nil
}

func handleRepairContentCommand(digest string, origin string, cc commandContext) (int, error) {
	if digest == "" && origin == "" {
		return 1, errDigestOrOriginIsRequired
	}

	nodes, err := cc.hashstate.GetOrderedNodes(digest, cc.appCfg.NumReplica)
	if err != nil {
		return 1, err
	}

	nodes = filterOutNodes(nodes, origin, cc)

	// oops: a given node is not in the list of nodes supposed to have the content digest
	// check if digest is hosted there and remove if necessary
	if len(nodes) == 0 && digest != "" {
		errCode, err := deleteContentItem(origin, digest, cc)
		return errCode, err
	}

	return repairContent(digest, origin, cc)
}

func deleteContentItem(originLabel string, digest string, cc commandContext) (int, error) {
	origin, ok := cc.nts[originLabel]
	if !ok {
		err := fmt.Errorf("could not find origin server for %s", originLabel)
		return 1, err
	}

	httpRequest, err := http.NewRequest("DELETE", "http://"+origin+"/blobs/"+digest, nil)
	if err != nil {
		return 1, err
	}

	httpClient := http.Client{}
	resp, err := httpClient.Do(httpRequest)
	if err != nil {
		return 1, err
	}

	if resp.StatusCode != 200 {
		err := fmt.Errorf("non 200 from origin %s for DELETE content "+
			"request %s, status code = %d, response = %s\n", origin, digest, resp.StatusCode, resp.Body)
		return 1, err
	}

	return 0, nil
}

func handleDeleteContentCommand(deleteContent string, origin string, cc commandContext) (int, error) {

	if deleteContent == "" {
		return 1, errDigestIsRequired
	}

	// get hashstate for a content item
	nodes, err := cc.hashstate.GetOrderedNodes(deleteContent, cc.appCfg.NumReplica)
	if err != nil {
		return 1, err
	}

	nodes = filterOutNodes(nodes, origin, cc)
	// iterate through all origins that may have the content and delete it
	for _, node := range nodes {
		deleted := "OK"
		if _, err := deleteContentItem(node.Label, deleteContent, cc); err != nil {
			deleted = "FAILED"

			if cc.verbose {
				// don't want to interrupt the loop so just log the error message
				fmt.Fprintln(cc.writer, err.Error())
			}
		}
		fmt.Fprintf(cc.writer, "origin: %s, content: %s, deleted: %s\n",
			node.Label, deleteContent, deleted)

	}

	return 0, nil
}

func handleNotImplementedCommand(flag string, origin string, cc commandContext) (int, error) {
	return 1, errNotImplemented
}

func handleNoOpCommand(flag string, origin string, cc commandContext) (int, error) {
	return 0, nil
}

func initHashState(appConfig cfg.AppConfig) *weightedhash.RendezvousHash {

	// initalize hashing state
	hs := weightedhash.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		weightedhash.UInt64ToFloat64)

	fmt.Println("Hash state is being initialized to: ")

	// Add all configured nodes to a hashing statae
	for origin, node := range appConfig.Hashstate {

		hs.AddNode(node.Label, node.Weight)
		fmt.Printf("Hash node added: [ origin: %s, label: %s, weight: %d ]\n", origin, node.Label, node.Weight)
	}

	if len(hs.Nodes) == 0 {
		panic("Hashstate has zero length: `0 any_operation X = 0`")
	}
	return hs
}

// CommandHandlerFunc defines a command handler type
type commandHandlerFunc func(flag string, origin string, cc commandContext) (int, error)

//RunMain main wrapper primarely for testing purposes. Please note you should not call os.Exit here
// this will screw tests up, call os.Exit only in a upper level function
func RunMain(cmdline []string, appConfig cfg.AppConfig, w io.Writer) int {

	if len(appConfig.Hashstate) == 0 {
		fmt.Fprintf(w, "No origin hash state configuraiton found. Major misconfiguraiton...")
	}

	context := commandContext{
		writer:    w,
		verbose:   false,
		nts:       make(nodeToServer),
		stn:       make(serverToNode),
		hashstate: initHashState(appConfig),
		appCfg:    appConfig}

	for origin, node := range appConfig.Hashstate {
		context.nts[node.Label] = origin
		context.stn[origin], _ = context.hashstate.GetNode(node.Label)
	}

	f := flag.NewFlagSet(cmdline[0], flag.ExitOnError)

	commands := map[string]struct {
		commandOpt  *string
		commandFunc commandHandlerFunc
	}{
		"origin": {
			f.String("origin", "", "Defines the scope of an operation for origin server, "+
				"should be use with an actual operation: info, delete, list or repair"),
			handleNoOpCommand,
		},
		"info": {
			f.String("info", "", "Lists origin servers, their capacity and utilization,"+
				"may be scoped to a single origin server via optional parameter -origin"),
			handleOriginListCommand,
		},
		"list": {
			f.String("list", "", "list: lists content on all origins if not specified,"+
				"may be scoped to a single origin server via optional parameter -origin"),
			handleContentListCommand,
		},
		"repair": {
			f.String("repair", "", "repair [content_digest]: make sure a given content "+
				"item is stored where it is supposed to, if that's not the case the routine will fix it "+
				"by copying other replicas, may be scoped to a single origin server via optional "+
				"parameter -origin"),
			handleRepairContentCommand,
		},
		"delete": {
			f.String("delete", "", "delete content_digest: Deletes all replicas of a content "+
				"and removes it from tracker metadata. Use with caution, may be scoped to "+
				"a single origin server via optional parameter -origin"),
			handleDeleteContentCommand,
		},
		"publish": {
			f.String("publish", "", "publish path_to_a_file_name: transfers content file into "+
				"a ring of origin servers and registers it as a torrent in a tracker. Not implemented yet"),
			handleNotImplementedCommand,
		},
		"download": {
			f.String("download", "./", "download content digest to a target path: "+
				"intiates P2P transfer of a content from origin servers to target_path. Not implemented yet"),
			handleNotImplementedCommand,
		},
	}

	if len(os.Args) < 2 {
		f.Usage()
		return 1
	}

	f.Parse(cmdline[1:])

	var errCode int
	var err error
	// Parse command line and run the command
	for command, commandStruct := range commands {
		if isFlagDefined(cmdline, command) {
			errCode, err = commandStruct.commandFunc(
				*commandStruct.commandOpt, *commands["origin"].commandOpt, context)
			if err != nil {
				fmt.Fprintf(w, "%s\n", err.Error())
			}
		}
	}

	return errCode
}
