package replication

import (
	rtUtils "github.com/jfrog/jfrog-cli-go/artifactory/utils"
	"github.com/jfrog/jfrog-cli-go/utils/config"
	"github.com/jfrog/jfrog-client-go/artifactory/services"
)

type ReplicationShowCommand struct {
	rtDetails  *config.ArtifactoryDetails
	showResult []services.PushReplicationParams
	repoKey    string
}

func NewReplicationShowCommand() *ReplicationShowCommand {
	return &ReplicationShowCommand{}
}

func (rsc *ReplicationShowCommand) SetRepoKey(repoKey string) *ReplicationShowCommand {
	rsc.repoKey = repoKey
	return rsc
}

func (rsc *ReplicationShowCommand) SetRtDetails(rtDetails *config.ArtifactoryDetails) *ReplicationShowCommand {
	rsc.rtDetails = rtDetails
	return rsc
}

func (rsc *ReplicationShowCommand) RtDetails() (*config.ArtifactoryDetails, error) {
	return rsc.rtDetails, nil
}

func (rsc *ReplicationShowCommand) CommandName() string {
	return "rt_replication_show"
}

func (rsc *ReplicationShowCommand) Run() error {
	servicesManager, err := rtUtils.CreateServiceManager(rsc.rtDetails, false)
	if err != nil {
		return err
	}
	rsc.showResult, err = servicesManager.ShowReplication(rsc.repoKey)
	return err
}

func (rsc *ReplicationShowCommand) ShowResult() []services.PushReplicationParams {
	return rsc.showResult
}

func (rsc *ReplicationShowCommand) RepoKey() string {
	return rsc.repoKey
}
