class KrakenBalances:
    def __init__(self, balances = {}):
        self.balances = balances #TODO maybe try {exchangeName: {balances}}

    def snapshot_balances(self, data):
        self.balances = {item["asset"]: item["balance"] for item in data}

    def update_balances(self, data):
        """Update balances based on incoming data."""
        for update in data:
            update_asset = update['asset']
            update_amount = update['amount']
            update_fee = update['fee']
            update_balance = update['balance']

            if update_balance != self.balances.get(update_asset, 0) + update_amount - update_fee:
                print("New {} balance doesn't match old balance + update - fee".format(update_asset))

            self.balances[update_asset] = update_balance

    def print_balances(self, exclude_balances=None):
        out = {asset: float(bal) for asset, bal in self.balances.items()}
        if exclude_balances:
            for asset, amt in exclude_balances.items():
                out[asset] = out.get(asset, 0.0) - float(amt)
        print('\n'.join(f"{asset}: {out[asset]:.8g}" for asset in sorted(out)) or "No balances available.")