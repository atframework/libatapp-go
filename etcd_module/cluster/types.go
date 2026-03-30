package cluster

type RoutingStrategy int

type ClusterState int

type ClusterFlag uint32

const (
	ClusterStateInitializing ClusterState = iota
	ClusterStateRunning
	ClusterStateStopping
	ClusterStateStopped
)

const (
	ClusterFlagClosing ClusterFlag = 0x0001
	ClusterFlagRunning ClusterFlag = 0x0002
	ClusterFlagReady   ClusterFlag = 0x0004

	ClusterFlagEnableLease            ClusterFlag = 0x0100
	ClusterFlagPreviousRequestTimeout ClusterFlag = 0x0200
)

const (
	RoutingStrategyConsistentHash RoutingStrategy = iota
	RoutingStrategyRoundRobin
	RoutingStrategyRandom
)
