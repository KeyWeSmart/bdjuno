package main

import (
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	junoMessages "github.com/forbole/juno/v4/modules/messages"
	nfttype "github.com/keywesmart/sesamenet/x/nft/types"
)

var sesamenetMessageAddressesParser = junoMessages.JoinMessageParsers(
	nftMessageAddressesParser,
)

func nftMessageAddressesParser(_ codec.Codec, cosmosMsg sdk.Msg) ([]string, error) {
	switch msg := cosmosMsg.(type) {
	case *nfttype.MsgIssueDenom:
		return []string{msg.Sender}, nil

	case *nfttype.MsgMintNFT:
		return []string{msg.Sender, msg.Recipient}, nil

	case *nfttype.MsgEditNFT:
		return []string{msg.Sender}, nil

	case *nfttype.MsgTransferNFT:
		return []string{msg.Sender, msg.Recipient}, nil

	case *nfttype.MsgBurnNFT:
		return []string{msg.Sender}, nil
	}

	return nil, junoMessages.MessageNotSupported(cosmosMsg)
}
