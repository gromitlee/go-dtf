package client

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/gromitlee/go-dtf/internal/db/model"
	"github.com/gromitlee/go-dtf/pkg/dtf_config"
)

type IDtfClient interface {
	CreateSAGA(ctx context.Context, sagasUUID string, typ uint32) error
	UpdateSAGAHeartbeat(ctx context.Context, sagasUUID string) error
	UpdateSAGAContext(ctx context.Context, sagasUUID, dbCtx string) error
	TransSAGAState(ctx context.Context, sagasUUID string, sagaEvent model.SAGAEvent) error
	CleanSAGA(ctx context.Context, timeout int64) (*model.DtfSaga, error)
}

type client struct {
	log dtf_config.Logger
	db  *gorm.DB
}

func NewClient(log dtf_config.Logger, db *gorm.DB) (IDtfClient, error) {
	if err := db.AutoMigrate(
		model.DtfSaga{},
	); err != nil {
		return nil, err
	}
	return &client{log: log, db: db}, nil
}

func (c *client) CreateSAGA(ctx context.Context, sagasUUID string, typ uint32) error {
	dbSaga := &model.DtfSaga{
		UUID:  sagasUUID,
		Typ:   typ,
		State: model.SAGAStateTransacting,
	}
	if err := c.db.WithContext(ctx).Create(dbSaga); err != nil {
		return nil
	}
	return nil
}

func (c *client) UpdateSAGAHeartbeat(ctx context.Context, sagasUUID string) error {
	return c.db.WithContext(ctx).Model(&model.DtfSaga{}).Where("uuid = ?", sagasUUID).Updates(map[string]interface{}{
		"updated_at": time.Now().UnixMilli(),
	}).Error
}

func (c *client) UpdateSAGAContext(ctx context.Context, sagasUUID, dbCtx string) error {
	return c.db.WithContext(ctx).Model(&model.DtfSaga{}).Where("uuid = ?", sagasUUID).Updates(map[string]interface{}{
		"ctx": dbCtx,
	}).Error
}

func (c *client) TransSAGAState(ctx context.Context, sagasUUID string, sagaEvent model.SAGAEvent) error {
	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		dbSaga := &model.DtfSaga{}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("uuid = ?", sagasUUID).First(dbSaga).Error; err != nil {
			return err
		}
		state, del, err := model.CheckTransfer(dbSaga.State, sagaEvent)
		if err != nil {
			return err
		}
		if del {
			return tx.Unscoped().Where("id = ?", dbSaga.ID).Delete(&model.DtfSaga{}).Error
		}
		return tx.Model(&model.DtfSaga{}).Where("id = ?", dbSaga.ID).Updates(map[string]interface{}{
			"state": state,
		}).Error
	})
}

func (c *client) CleanSAGA(ctx context.Context, timeout int64) (*model.DtfSaga, error) {
	var dbSaga *model.DtfSaga
	updateLimit := time.Now().Add(-time.Duration(timeout) * time.Millisecond).UnixMilli()
	return dbSaga, c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var dbSagas []*model.DtfSaga
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("state = ?", model.SAGAStateTransacting).
			Where("updated_at < ?", updateLimit).
			Order("id").Limit(1).Find(&dbSagas).Error; err != nil {
			return err
		} else if len(dbSagas) == 0 {
			return nil
		} else {
			dbSaga = dbSagas[0]
		}
		if err := tx.Model(&model.DtfSaga{}).Where("id = ?", dbSaga.ID).Updates(map[string]interface{}{
			"state": model.SAGAStateCompensating,
		}).Error; err != nil {
			return err
		}
		c.log.Errorf("sagas %v typ %v transacting but cleaned, last updated at %v", dbSaga.UUID, dbSaga.Typ, dbSaga.UpdatedAt)
		return nil
	})
}
