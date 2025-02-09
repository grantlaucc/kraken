class KrakenBalances:
    def __init__(self, balances = {}):
        self.balances = balances #TODO maybe try {exchangeName: {balances}}

    def print_balances(self):
        print('\n'.join(f"{asset}: {bal}" for asset, bal in self.balances.items()) 
              or "No balances available.")