package database

import (
	"fmt"

	dbtypes "github.com/forbole/bdjuno/v3/database/types"
	dbutils "github.com/forbole/bdjuno/v3/database/utils"
	"github.com/forbole/bdjuno/v3/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/lib/pq"
)

// SaveSupply allows to save for the given height the given total amount of coins
func (db *Db) SaveSupply(coins sdk.Coins, height int64) error {
	query := `
INSERT INTO supply (coins, height) 
VALUES ($1, $2) 
ON CONFLICT (one_row_id) DO UPDATE 
    SET coins = excluded.coins,
    	height = excluded.height
WHERE supply.height <= excluded.height`

	_, err := db.Sql.Exec(query, pq.Array(dbtypes.NewDbCoins(coins)), height)
	if err != nil {
		return fmt.Errorf("error while storing supply: %s", err)
	}

	return nil
}

// SaveAccountBalances allows to store the given balances inside the database
func (db *Db) SaveAccountBalances(balances []types.AccountBalance) error {
	paramsNumber := 3
	slices := dbutils.SplitBalances(balances, paramsNumber)

	for _, balances := range slices {
		if len(balances) == 0 {
			continue
		}

		// Store up-to-date data
		err := db.saveUpToDateBalances(paramsNumber, balances)
		if err != nil {
			return fmt.Errorf("error while storing up-to-date balances: %s", err)
		}
	}

	return nil
}

func (db *Db) saveUpToDateBalances(paramsNumber int, balances []types.AccountBalance) error {
	stmt := `INSERT INTO account_balance (address, coins, height) VALUES `
	var params []interface{}

	for i, bal := range balances {
		bi := i * paramsNumber
		stmt += fmt.Sprintf("($%d, $%d, $%d),", bi+1, bi+2, bi+3)

		available := pq.Array(dbtypes.NewDbCoins(bal.Balance))
		params = append(params, bal.Address, available, bal.Height)
	}

	stmt = stmt[:len(stmt)-1]
	stmt += `
ON CONFLICT (address) DO UPDATE 
	SET coins = excluded.coins, 
	    height = excluded.height 
WHERE account_balance.height <= excluded.height`

	_, err := db.Sql.Exec(stmt, params...)
	if err != nil {
		return err
	}
	return nil
}
