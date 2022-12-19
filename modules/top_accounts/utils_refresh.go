package top_accounts

import (
	"fmt"

	juno "github.com/forbole/juno/v3/types"
	"github.com/rs/zerolog/log"
)

func (m *Module) refreshTopAccountsSum(addresses []string) error {
	for _, addr := range addresses {
		sum, err := m.db.GetAccountBalanceSum(addr)
		if err != nil {
			return fmt.Errorf("error while getting account balance sum : %s", err)
		}

		err = m.db.UpdateTopAccountsSum(addr, sum)
		if err != nil {
			return fmt.Errorf("error while updating top accounts sum : %s", err)
		}
	}
	return nil
}

func (m *Module) refreshDelegations(height int64, delegator string) func() {
	return func() {
		err := m.stakingModule.RefreshDelegations(height, delegator)
		if err != nil {
			log.Error().Str("module", "top acconts").Err(err).
				Str("operation", "refresh delegations").Msg("error while refreshing delegations")
		}
	}
}

func (m *Module) refreshRedelegations(tx *juno.Tx, index int, delegatorAddr string) func() {
	return func() {
		err := m.stakingModule.RefreshRedelegations(tx, index, delegatorAddr)
		if err != nil {
			log.Error().Str("module", "top acconts").Err(err).
				Str("operation", "refresh delegations").Msg("error while refreshing delegations")
		}
	}
}