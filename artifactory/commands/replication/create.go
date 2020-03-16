package replication

import (
	"encoding/json"
	"errors"

	"github.com/jfrog/jfrog-cli-go/artifactory/commands/utils"
	rtUtils "github.com/jfrog/jfrog-cli-go/artifactory/utils"
	"github.com/jfrog/jfrog-cli-go/utils/cliutils"
	"github.com/jfrog/jfrog-cli-go/utils/config"
	"github.com/jfrog/jfrog-client-go/artifactory/services"
	"github.com/jfrog/jfrog-client-go/utils/errorutils"
	"github.com/jfrog/jfrog-client-go/utils/io/fileutils"
)

type ReplicationCreateCommand struct {
	rtDetails    *config.ArtifactoryDetails
	templatePath string
	vars         string
}

func NewReplicationCreateCommand() *ReplicationCreateCommand {
	return &ReplicationCreateCommand{}
}

func (rcc *ReplicationCreateCommand) SetTemplatePath(path string) *ReplicationCreateCommand {
	rcc.templatePath = path
	return rcc
}

func (rcc *ReplicationCreateCommand) SetVars(vars string) *ReplicationCreateCommand {
	rcc.vars = vars
	return rcc
}

func (rcc *ReplicationCreateCommand) SetRtDetails(rtDetails *config.ArtifactoryDetails) *ReplicationCreateCommand {
	rcc.rtDetails = rtDetails
	return rcc
}

func (rcc *ReplicationCreateCommand) RtDetails() (*config.ArtifactoryDetails, error) {
	return rcc.rtDetails, nil
}

func (rcc *ReplicationCreateCommand) CommandName() string {
	return "rt_replication_create"
}

func (rcc *ReplicationCreateCommand) Run() (err error) {
	content, err := fileutils.ReadFile(rcc.templatePath)
	if errorutils.CheckError(err) != nil {
		return
	}
	if len(rcc.vars) > 0 {
		templateVars := cliutils.SpecVarsStringToMap(rcc.vars)
		content = cliutils.ReplaceSpecVars(content, templateVars)
	}
	var replicationConfigMap map[string]interface{}
	err = json.Unmarshal(content, &replicationConfigMap)
	if errorutils.CheckError(err) != nil {
		return
	}
	var jobType string
	for key, value := range replicationConfigMap {
		switch key {
		case "jobType":
			jobType = value.(string)
		default:
			writertsMap[key](&replicationConfigMap, key, value.(string))
		}
	}
	fillMissingDefaultValue(replicationConfigMap)
	content, err = json.Marshal(replicationConfigMap)
	return createReplication(jobType, rcc.rtDetails, content)
}

func createReplication(jobType string, rtDetails *config.ArtifactoryDetails, jsonConfig []byte) error {
	servicesManager, err := rtUtils.CreateServiceManager(rtDetails, false)
	if err != nil {
		return err
	}
	switch {
	case jobType == Pull:
		params, err := getPullParams(jsonConfig)
		if err != nil {
			return err
		}
		return servicesManager.CreatePullReplication(*params)
	case jobType == Push:
		params, err := getPushParams(jsonConfig)
		if err != nil {
			return err
		}
		return servicesManager.CreatePushReplication(*params)
	}
	return errorutils.CheckError(errors.New("Cloud not detect replication job type "))
}

func getRepoKey(jsonConfig []byte) (string, error) {
	var params services.CommonReplicationParams
	err := json.Unmarshal(jsonConfig, &params)
	if err != nil {
		return "", err
	}
	return params.RepoKey, nil
}

func getPullParams(jsonConfig []byte) (*services.PullReplicationParams, error) {
	var params *services.PullReplicationParams
	err := json.Unmarshal(jsonConfig, &params)
	if err != nil {
		return nil, err
	}
	return params, err
}

func getPushParams(jsonConfig []byte) (*services.PushReplicationParams, error) {
	var params *services.PushReplicationParams
	err := json.Unmarshal(jsonConfig, &params)
	if err != nil {
		return nil, err
	}
	return params, err
}

func fillMissingDefaultValue(replicationConfigMap map[string]interface{}) {
	if _, ok := replicationConfigMap["socketTimeoutMillis"]; !ok {
		writertsMap["socketTimeoutMillis"](&replicationConfigMap, "socketTimeoutMillis", "15000")
	}
	if _, ok := replicationConfigMap["syncProperties"]; !ok {
		writertsMap["syncProperties"](&replicationConfigMap, "syncProperties", "true")
	}
}

var writertsMap = map[string]utils.AnswerWriter{
	Username:               utils.WriteStringAnswer,
	Password:               utils.WriteStringAnswer,
	URL:                    utils.WriteStringAnswer,
	RepoKey:                utils.WriteStringAnswer,
	CronExp:                utils.WriteStringAnswer,
	EnableEventReplication: utils.WriteBoolAnswer,
	Enabled:                utils.WriteBoolAnswer,
	SyncDeletes:            utils.WriteBoolAnswer,
	SyncProperties:         utils.WriteBoolAnswer,
	SyncStatistics:         utils.WriteBoolAnswer,
	PathPrefix:             utils.WriteStringAnswer,
	SocketTimeoutMillis:    utils.WriteIntAnswer,
}
