// Package registration provides service registration and lease management.
package registration

import (
	"context"
	"errors"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/atframework/libatapp-go/etcd_module/client"
	internalcodec "github.com/atframework/libatapp-go/etcd_module/internal/codec"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

const defaultRegisterTimeout = 10 * time.Second

// LeaseOwner exposes approved lease ID for keepalive actors.
type LeaseOwner interface {
	GetLease() clientv3.LeaseID
}

// RegistrationState 定义KeepaliveState类型。
type RegistrationState int

const (
	RegistrationInitializing RegistrationState = iota
	RegistrationActive
	RegistrationReconnecting
	RegistrationFailed
	RegistrationStopped
)

// EtcdRegistration 定义EtcdKeepalive类型。
type EtcdRegistration struct {
	etcdClient client.EtcdClient
	logger     *log.Logger
	info       *pb.AtappDiscovery
	leaseOwner LeaseOwner

	path string

	ttl int64

	isClosed bool
	hasData  bool
	mu       sync.RWMutex
	state    RegistrationState

	checker       func(existingValue string, currentInfo *pb.AtappDiscovery) bool
	checkRun      bool
	checkPassed   bool
	checkRequired bool

	// Value change tracking
	valueChanged bool
	lastValue    string
	lastArea     *pb.AtappArea
	lastMetadata *pb.AtappMetadata
	lastIdentity string

	// Concurrency control using singleflight for deduplication
	unregisterGroup singleflight.Group
	refreshGroup    singleflight.Group
}

// NewEtcdRegistration 创建并返回EtcdKeepalive。
func NewEtcdRegistration(info *pb.AtappDiscovery, path string, ttl int64, etcdClient client.EtcdClient, logger *log.Logger, leaseOwner LeaseOwner) (*EtcdRegistration, error) {
	if etcdClient == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}
	if info == nil {
		return nil, fmt.Errorf("service info is nil")
	}
	if path == "" {
		return nil, fmt.Errorf("service path is empty")
	}
	if logger == nil {
		logger = log.Default()
	}

	infoCopy := proto.Clone(info).(*pb.AtappDiscovery)

	return &EtcdRegistration{
		etcdClient:    etcdClient,
		logger:        logger,
		info:          infoCopy,
		leaseOwner:    leaseOwner,
		path:          path,
		ttl:           ttl,
		isClosed:      true,
		hasData:       false,
		state:         RegistrationInitializing,
		checker:       defaultRegistrationChecker,
		checkRun:      false,
		checkPassed:   false,
		checkRequired: true,
		valueChanged:  true,
		lastArea:      cloneArea(infoCopy.Area),
		lastMetadata:  cloneMetadata(infoCopy.Metadata),
		lastIdentity:  infoCopy.Identity,
	}, nil
}

// GetState 获取State。
func (s *EtcdRegistration) GetState() RegistrationState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// GetPath 获取Path。
func (s *EtcdRegistration) GetPath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.path
}

// GetTTL returns configured TTL for C++ semantic parity review.
func (s *EtcdRegistration) GetTTL() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ttl
}

// GetServiceInfoCopy returns a cloned service info for C++ actor-style APIs.
func (s *EtcdRegistration) GetServiceInfoCopy() *pb.AtappDiscovery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.info == nil {
		return nil
	}
	if cloned, ok := proto.Clone(s.info).(*pb.AtappDiscovery); ok {
		return cloned
	}
	return s.info
}

// GetLeaseID 获取LeaseID。
func (s *EtcdRegistration) GetLeaseID() clientv3.LeaseID {
	s.mu.RLock()
	owner := s.leaseOwner
	s.mu.RUnlock()

	if owner == nil {
		return 0
	}
	return owner.GetLease()
}

// HasData 判断是否存在Data。
func (s *EtcdRegistration) HasData() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hasData
}

// SetChecker allows overriding pre-write checker for C++ parity behavior.
func (s *EtcdRegistration) SetChecker(checker func(existingValue string, currentInfo *pb.AtappDiscovery) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if checker == nil {
		s.checker = defaultRegistrationChecker
	} else {
		s.checker = checker
	}
	s.checkRequired = true
	s.checkRun = false
	s.checkPassed = false
}

// SetCheckerFromValue sets a strict checker against a previous value snapshot.
func (s *EtcdRegistration) SetCheckerFromValue(expected string) {
	s.SetChecker(func(existingValue string, _ *pb.AtappDiscovery) bool {
		return expected == "" || existingValue == expected
	})
}

// IsCheckRun reports whether pre-write checker has been executed.
func (s *EtcdRegistration) IsCheckRun() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkRun
}

// IsCheckPassed reports latest checker result.
func (s *EtcdRegistration) IsCheckPassed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkPassed
}

// ForceCheckOnNextUpdate resets checker state and requires next write pre-check.
func (s *EtcdRegistration) ForceCheckOnNextUpdate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkRequired = true
	s.checkRun = false
	s.checkPassed = false
}

// GetValue 获取Value。
func (s *EtcdRegistration) GetValue() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastValue
}

// UpdateServiceInfo 更新ServiceInfo。
func (s *EtcdRegistration) UpdateServiceInfo(info *pb.AtappDiscovery) error {
	return s.UpdateServiceInfoWithContext(context.Background(), info)
}

// UpdateServiceInfoWithContext 更新ServiceInfoWithContext。
func (s *EtcdRegistration) UpdateServiceInfoWithContext(ctx context.Context, info *pb.AtappDiscovery) error {
	s.mu.Lock()
	if info == nil {
		s.mu.Unlock()
		return fmt.Errorf("service info is nil")
	}
	infoCopy := proto.Clone(info).(*pb.AtappDiscovery)
	changed := s.diffDiscoveryInfo(infoCopy)
	s.info = infoCopy
	if changed {
		s.valueChanged = true
		s.lastArea = cloneArea(infoCopy.Area)
		s.lastMetadata = cloneMetadata(infoCopy.Metadata)
		s.lastIdentity = infoCopy.Identity
	}
	s.mu.Unlock()

	if changed {
		return s.refresh(ctx)
	}
	return nil
}

// TriggerMaybeUpdate 实现。
func (s *EtcdRegistration) TriggerMaybeUpdate(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return nil
	}
	s.valueChanged = true
	s.mu.Unlock()

	return s.refresh(ctx)
}

func (s *EtcdRegistration) diffDiscoveryInfo(info *pb.AtappDiscovery) bool {
	if info == nil {
		return false
	}
	if s.lastIdentity != info.Identity {
		return true
	}
	if !areaEqual(s.lastArea, info.Area) {
		return true
	}
	if !metadataEqual(s.lastMetadata, info.Metadata) {
		return true
	}
	return s.valueChanged
}

func areaEqual(left, right *pb.AtappArea) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	return left.Region == right.Region && left.District == right.District && left.ZoneId == right.ZoneId
}

func metadataEqual(left, right *pb.AtappMetadata) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	if left.ApiVersion != right.ApiVersion || left.Kind != right.Kind || left.Group != right.Group {
		return false
	}
	if left.Name != right.Name || left.NamespaceName != right.NamespaceName || left.Uid != right.Uid {
		return false
	}
	if left.ServiceSubset != right.ServiceSubset {
		return false
	}
	if len(left.Labels) != len(right.Labels) {
		return false
	}
	for key, value := range left.Labels {
		if right.Labels[key] != value {
			return false
		}
	}
	return true
}

func cloneArea(area *pb.AtappArea) *pb.AtappArea {
	if area == nil {
		return nil
	}
	return &pb.AtappArea{
		Region:   area.Region,
		District: area.District,
		ZoneId:   area.ZoneId,
	}
}

func cloneMetadata(metadata *pb.AtappMetadata) *pb.AtappMetadata {
	if metadata == nil {
		return nil
	}
	labels := make(map[string]string, len(metadata.Labels))
	for key, value := range metadata.Labels {
		labels[key] = value
	}
	return &pb.AtappMetadata{
		ApiVersion:    metadata.ApiVersion,
		Kind:          metadata.Kind,
		Group:         metadata.Group,
		Name:          metadata.Name,
		NamespaceName: metadata.NamespaceName,
		Uid:           metadata.Uid,
		ServiceSubset: metadata.ServiceSubset,
		Labels:        labels,
	}
}

// Start starts registration using lease from LeaseOwner.
func (s *EtcdRegistration) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	approvedLeaseID := s.GetLeaseID()
	if approvedLeaseID == 0 {
		return fmt.Errorf("approved lease is unavailable")
	}

	s.mu.Lock()
	if !s.isClosed {
		s.mu.Unlock()
		return fmt.Errorf("service at path %s is already registered", s.path)
	}
	s.isClosed = false
	s.hasData = false
	s.state = RegistrationInitializing
	s.valueChanged = true
	s.checkRun = false
	s.checkPassed = false
	s.checkRequired = true
	s.mu.Unlock()

	attemptCtx, cancel := context.WithTimeout(ctx, defaultRegisterTimeout)
	defer cancel()
	if err := s.registerOnce(attemptCtx, approvedLeaseID); err != nil {
		s.mu.Lock()
		s.isClosed = true
		s.hasData = false
		s.state = RegistrationFailed
		s.mu.Unlock()
		return err
	}
	s.mu.Lock()
	s.state = RegistrationActive
	s.mu.Unlock()
	return nil
}

// Unregister 实现。
func (s *EtcdRegistration) Unregister(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, err, _ := s.unregisterGroup.Do("unregister", func() (interface{}, error) {
		s.mu.Lock()
		if s.isClosed {
			s.mu.Unlock()
			s.logger.Warn("Service already unregistered or was never registered", "path", s.path)
			return nil, nil
		}
		path := s.path
		s.mu.Unlock()

		_, delErr := s.etcdClient.Delete(ctx, path)

		s.mu.Lock()
		defer s.mu.Unlock()
		if delErr != nil {
			s.logger.Error("Failed to delete service key", "error", delErr, "path", path)
			return nil, delErr
		}

		s.isClosed = true
		s.hasData = false
		s.state = RegistrationStopped
		s.logger.Info("Service unregistered successfully", "path", path)
		return nil, nil
	})
	return err
}

func (s *EtcdRegistration) markStopped() {
	s.mu.Lock()
	s.isClosed = true
	s.hasData = false
	s.state = RegistrationStopped
	s.mu.Unlock()
}

// Stop 停止Stop。
func (s *EtcdRegistration) Stop(ctx context.Context) error {
	return s.Unregister(ctx)
}

// CloseWithReset closes keepalive and optionally clears hasData state.
func (s *EtcdRegistration) CloseWithReset(resetHasData bool) error {
	err := s.Unregister(context.Background())
	if !resetHasData {
		s.mu.Lock()
		s.hasData = true
		s.mu.Unlock()
	}
	return err
}

// Close 关闭模块并释放底层资源。
func (s *EtcdRegistration) Close() error {
	return s.CloseWithReset(true)
}

type refreshSnapshot struct {
	leaseID      clientv3.LeaseID
	etcdClient   client.EtcdClient
	path         string
	info         *pb.AtappDiscovery
	valueChanged bool
}

type registerSnapshot struct {
	leaseID    clientv3.LeaseID
	etcdClient client.EtcdClient
	path       string
	info       *pb.AtappDiscovery
}

func (s *EtcdRegistration) snapshotForRefresh() (refreshSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isClosed {
		return refreshSnapshot{}, fmt.Errorf("service is not registered")
	}

	if s.info == nil {
		return refreshSnapshot{}, fmt.Errorf("service info is nil")
	}

	// Read leaseOwner directly (lock already held) to avoid calling GetLeaseID()
	// which takes a second RLock and can deadlock when a writer is waiting.
	var leaseID clientv3.LeaseID
	if s.leaseOwner != nil {
		leaseID = s.leaseOwner.GetLease()
	}

	return refreshSnapshot{
		leaseID:      leaseID,
		etcdClient:   s.etcdClient,
		path:         s.path,
		info:         proto.Clone(s.info).(*pb.AtappDiscovery),
		valueChanged: s.valueChanged,
	}, nil
}

// refresh 实现。
func (s *EtcdRegistration) refresh(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, err, _ := s.refreshGroup.Do("refresh", func() (interface{}, error) {
		snapshot, err := s.snapshotForRefresh()
		if err != nil {
			return nil, err
		}

		if snapshot.leaseID == 0 {
			return nil, fmt.Errorf("leaseID is required for service registration")
		}

		if !snapshot.valueChanged {
			return nil, nil
		}

		err = s.doRefreshEffect(ctx, snapshot)

		s.mu.Lock()
		defer s.mu.Unlock()
		if err == nil {
			s.valueChanged = false
		}

		return nil, err
	})
	return err
}

func (s *EtcdRegistration) doRefreshEffect(ctx context.Context, snapshot refreshSnapshot) error {
	if err := s.validateIdentityForWrite(snapshot.info); err != nil {
		return err
	}

	if err := s.runCheckBeforeUpdate(ctx, snapshot.etcdClient, snapshot.path, snapshot.info); err != nil {
		return err
	}

	jsonValue, err := s.marshalInfoForWrite(snapshot.info)
	if err != nil {
		return err
	}

	if err := s.putWithLease(ctx, snapshot.etcdClient, snapshot.path, snapshot.leaseID, jsonValue, "Failed to refresh service info"); err != nil {
		return err
	}

	s.mu.Lock()
	s.lastValue = string(jsonValue)
	if !s.isClosed {
		s.hasData = true
	}
	s.mu.Unlock()

	s.logger.Info("Service info refreshed", "path", snapshot.path)
	return nil
}

func (s *EtcdRegistration) snapshotForRegister() (registerSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.info == nil {
		s.logger.Error("Service info is nil")
		return registerSnapshot{}, errors.New("service info is nil")
	}

	var leaseID clientv3.LeaseID
	if s.leaseOwner != nil {
		leaseID = s.leaseOwner.GetLease()
	}

	return registerSnapshot{
		leaseID:    leaseID,
		etcdClient: s.etcdClient,
		path:       s.path,
		info:       proto.Clone(s.info).(*pb.AtappDiscovery),
	}, nil
}

func (s *EtcdRegistration) validateIdentityForWrite(info *pb.AtappDiscovery) error {
	if info.Identity == "" {
		s.logger.Error("Identity must be non-empty for service registration")
		return errors.New("identity must be non-empty for service registration")
	}
	return nil
}

func (s *EtcdRegistration) marshalInfoForWrite(info *pb.AtappDiscovery) ([]byte, error) {
	jsonValue, err := internalcodec.MarshalDiscoveryToJSON(info)
	if err != nil {
		s.logger.Error("Failed to marshal service info to JSON", "error", err)
		return nil, err
	}
	return jsonValue, nil
}

func (s *EtcdRegistration) storeLastValue(jsonValue []byte) {
	s.mu.Lock()
	s.lastValue = string(jsonValue)
	s.mu.Unlock()
}

func (s *EtcdRegistration) putWithLease(ctx context.Context, etcdClient client.EtcdClient, path string, leaseID clientv3.LeaseID, jsonValue []byte, errorMsg string) error {
	if _, err := etcdClient.Put(ctx, path, string(jsonValue), clientv3.WithLease(leaseID)); err != nil {
		s.logger.Error(errorMsg, "error", err, "path", path)
		return err
	}
	return nil
}

func (s *EtcdRegistration) registerOnce(ctx context.Context, leaseID clientv3.LeaseID) error {
	snapshot, err := s.snapshotForRegister()
	if err != nil {
		return err
	}
	snapshot.leaseID = leaseID

	if err := s.validateIdentityForWrite(snapshot.info); err != nil {
		return err
	}

	jsonValue, err := s.marshalInfoForWrite(snapshot.info)
	if err != nil {
		return err
	}

	if err := s.runCheckBeforeUpdate(ctx, snapshot.etcdClient, snapshot.path, snapshot.info); err != nil {
		return err
	}

	if snapshot.leaseID == 0 {
		return fmt.Errorf("leaseID is required for service registration")
	}

	if err := s.putWithLease(ctx, snapshot.etcdClient, snapshot.path, snapshot.leaseID, jsonValue, "Failed to put service key-value to etcd"); err != nil {
		return err
	}

	s.storeLastValue(jsonValue)
	s.mu.Lock()
	s.hasData = true
	s.valueChanged = false
	s.mu.Unlock()

	s.logRegistration(snapshot.path, snapshot.info, snapshot.leaseID)
	return nil
}

func (s *EtcdRegistration) logRegistration(path string, info *pb.AtappDiscovery, leaseID clientv3.LeaseID) {
	s.logger.Info("Service registered successfully",
		"path", path,
		"name", info.Name,
		"leaseID", int64(leaseID),
		"ttl", s.ttl,
	)
}

func (s *EtcdRegistration) checkIdentityCollision(checkedValue string, currentInfo *pb.AtappDiscovery) error {
	if checkedValue == "" || currentInfo == nil || currentInfo.Identity == "" {
		return nil
	}

	var existingDiscovery pb.AtappDiscovery
	if err := internalcodec.UnmarshalDiscoveryFromPayload([]byte(checkedValue), &existingDiscovery); err != nil {
		return nil
	}
	if existingDiscovery.Identity != "" && existingDiscovery.Identity != currentInfo.Identity {
		return fmt.Errorf("identity collision: cannot overwrite service with different identity (existing: %s, new: %s)",
			existingDiscovery.Identity, currentInfo.Identity)
	}
	return nil
}

func defaultRegistrationChecker(checkedValue string, currentInfo *pb.AtappDiscovery) bool {
	if checkedValue == "" || currentInfo == nil || currentInfo.Identity == "" {
		return true
	}

	var existingDiscovery pb.AtappDiscovery
	if err := internalcodec.UnmarshalDiscoveryFromPayload([]byte(checkedValue), &existingDiscovery); err != nil {
		return true
	}
	if existingDiscovery.Identity != "" && existingDiscovery.Identity != currentInfo.Identity {
		return false
	}
	return true
}

func (s *EtcdRegistration) runCheckBeforeUpdate(ctx context.Context, etcdClient client.EtcdClient, path string, currentInfo *pb.AtappDiscovery) error {
	s.mu.RLock()
	requireCheck := s.checkRequired
	checker := s.checker
	s.mu.RUnlock()

	if !requireCheck {
		return nil
	}
	if checker == nil {
		checker = defaultRegistrationChecker
	}

	resp, err := etcdClient.Get(ctx, path)
	if err != nil {
		s.logger.Error("Failed to check existing value", "error", err, "path", path)
		return err
	}

	var checkedValue string
	if len(resp.Kvs) > 0 {
		checkedValue = string(resp.Kvs[0].Value)
	}

	passed := checker(checkedValue, currentInfo)
	s.mu.Lock()
	s.checkRun = true
	s.checkPassed = passed
	if passed {
		s.checkRequired = false
	}
	s.mu.Unlock()

	if !passed {
		if err := s.checkIdentityCollision(checkedValue, currentInfo); err != nil {
			return err
		}
		return fmt.Errorf("checker rejected keepalive update for path %s", path)
	}

	return nil
}

func (s *EtcdRegistration) setState(state RegistrationState) {
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
}

// RegistrationManager 定义KeepaliveManager管理器结构。
type RegistrationManager struct {
	logger        *log.Logger
	keepalives    map[string]*EtcdRegistration
	approvedLease clientv3.LeaseID
	mu            sync.RWMutex
}

// NewRegistrationManager 创建并返回KeepaliveManager。
func NewRegistrationManager(logger *log.Logger) *RegistrationManager {
	if logger == nil {
		logger = log.Default()
	}
	return &RegistrationManager{
		logger:     logger,
		keepalives: make(map[string]*EtcdRegistration),
	}
}

// AddRegistration 添加Keepalive。
func (m *RegistrationManager) AddRegistration(service *EtcdRegistration) {
	_ = m.AddRegistrationIfAbsent(service)
}

// AddRegistrationIfAbsent adds keepalive when key is absent.
func (m *RegistrationManager) AddRegistrationIfAbsent(service *EtcdRegistration) bool {
	if service == nil {
		return false
	}
	path := service.GetPath()
	if path == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.keepalives[path]; ok {
		return false
	}
	if service.leaseOwner == nil {
		service.leaseOwner = m
	}
	m.keepalives[path] = service
	return true
}

// RemoveRegistration 移除Keepalive。
func (m *RegistrationManager) RemoveRegistration(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keepalives, path)
}

// GetRegistration 获取Keepalive。
func (m *RegistrationManager) GetRegistration(path string) (*EtcdRegistration, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	service, ok := m.keepalives[path]
	return service, ok
}

// GetAllRegistrations 获取AllKeepalives。
func (m *RegistrationManager) GetAllRegistrations() []*EtcdRegistration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := make([]*EtcdRegistration, 0, len(m.keepalives))
	for _, svc := range m.keepalives {
		items = append(items, svc)
	}
	return items
}

// RemoveRegistrationAndIsEmpty atomically removes a registration and reports whether manager becomes empty.
func (m *RegistrationManager) RemoveRegistrationAndIsEmpty(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keepalives, path)
	return len(m.keepalives) == 0
}

// UnregisterAll 实现。
func (m *RegistrationManager) UnregisterAll(ctx context.Context) error {
	m.mu.Lock()
	services := make([]*EtcdRegistration, 0, len(m.keepalives))
	for _, service := range m.keepalives {
		services = append(services, service)
	}
	m.keepalives = make(map[string]*EtcdRegistration)
	m.mu.Unlock()

	for _, service := range services {
		if err := service.Unregister(ctx); err != nil {
			m.logger.Error("Failed to unregister service", "path", service.GetPath(), "error", err)
		}
	}

	return nil
}

// Close 关闭模块并释放底层资源。
func (m *RegistrationManager) Close(ctx context.Context) error {
	return m.UnregisterAll(ctx)
}

// GetLease 获取Lease。
func (m *RegistrationManager) GetLease() clientv3.LeaseID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.approvedLease
}

// SetLease 设置Lease。
func (m *RegistrationManager) SetLease(leaseID clientv3.LeaseID) {
	m.mu.Lock()
	m.approvedLease = leaseID
	m.mu.Unlock()

	m.mu.RLock()
	actors := make([]*EtcdRegistration, 0, len(m.keepalives))
	for _, svc := range m.keepalives {
		actors = append(actors, svc)
	}
	m.mu.RUnlock()

	for _, actor := range actors {
		if actor == nil {
			continue
		}
		if actor.GetState() != RegistrationStopped {
			if err := actor.TriggerMaybeUpdate(context.Background()); err != nil {
				m.logger.Warn("failed to refresh keepalive after manager lease update", "path", actor.GetPath(), "error", err)
			}
		}
	}
}

// TriggerMaybeUpdateAll 实现。
func (m *RegistrationManager) TriggerMaybeUpdateAll(ctx context.Context) error {
	m.mu.RLock()
	actors := make([]*EtcdRegistration, 0, len(m.keepalives))
	for _, svc := range m.keepalives {
		actors = append(actors, svc)
	}
	m.mu.RUnlock()

	var errs []error
	for _, actor := range actors {
		if err := actor.TriggerMaybeUpdate(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// ResetChecksAll requests all actors to rerun pre-write checker on next update.
func (m *RegistrationManager) ResetChecksAll() {
	m.mu.RLock()
	actors := make([]*EtcdRegistration, 0, len(m.keepalives))
	for _, svc := range m.keepalives {
		actors = append(actors, svc)
	}
	m.mu.RUnlock()

	for _, actor := range actors {
		if actor == nil {
			continue
		}
		actor.ForceCheckOnNextUpdate()
	}
}

// ActiveAll starts non-active keepalive actors with current lease and refreshes active ones.
func (m *RegistrationManager) ActiveAll(ctx context.Context) error {
	m.mu.RLock()
	actors := make([]*EtcdRegistration, 0, len(m.keepalives))
	for _, svc := range m.keepalives {
		actors = append(actors, svc)
	}
	m.mu.RUnlock()

	var errs []error
	for _, actor := range actors {
		if actor == nil {
			continue
		}

		if actor.GetState() == RegistrationActive {
			if err := actor.TriggerMaybeUpdate(ctx); err != nil {
				errs = append(errs, err)
			}
			continue
		}

		if err := actor.Start(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
