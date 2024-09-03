import json
from websocket import WebSocketApp
import collections


class DepthSnapshot:
    """Class to manage and update market depth snapshots."""

    def __init__(self):
        """Initialize ordered dictionaries for bids and asks."""
        self.bids = collections.OrderedDict()
        self.asks = collections.OrderedDict()

    @staticmethod
    def update_items(items, msg_side):
        """Update bid or ask items based on incoming message data.

        Args:
            items (OrderedDict): Current bid or ask items.
            msg_side (list): Incoming bid or ask data.
        """
        for item in msg_side:
            price = item['Price']
            volume = item['Volume']
            if volume > 0:
                items[price] = volume
            else:
                items.pop(price, None)

    def update(self, msg):
        """Update the market depth snapshot based on a new message.

        Args:
            msg (dict): Incoming market depth message.
        """
        market_depth = msg['MarketDepth']
        if not market_depth['IsUpdate']:
            # If it's not an update, reset the snapshot
            self.bids.clear()
            self.asks.clear()
        DepthSnapshot.update_items(self.bids, market_depth['Bids'])
        DepthSnapshot.update_items(self.asks, market_depth['Asks'])
        print("Best bid:", max(self.bids) if self.bids else "None",
              "Best ask:", min(self.asks) if self.asks else "None")

    def print_state(self):
        """Print the current state of bids and asks."""
        print('Bids:', self.bids)
        print('Asks:', self.asks)
        if self.bids and self.asks:
            print('Best Bid/Ask:', max(self.bids), '/', min(self.asks))
        else:
            print('No valid bids or asks.')


# List of symbols to subscribe for market data
symbols = ['BTC-PERPETUAL@DERIBIT', 'BTC-USD@DYDX_V4', 'BTCUSDT@BYBIT_SPOT']


def on_error(ws, error):
    """Handle errors in the WebSocket connection.

    Args:
        ws (WebSocketApp): WebSocket application instance.
        error (str): Error message.
    """
    print("WebSocket error:", error)


def on_open(ws):
    """Handle WebSocket connection open event.

    Args:
        ws (WebSocketApp): WebSocket application instance.
    """
    message = {"Message": {"SymbolsRequest": {}}}
    ws.send(json.dumps(message))

    for symbol in symbols:
        # Subscribe to symbols request on open
        depth = json.dumps({'Message': {'MarketDepth': {'Symbol': symbol}}})
        trade = json.dumps({'Message': {'PublicTrades': {'Symbol': symbol}}})
        ws.send(depth)
        ws.send(trade)


# Dictionary to keep track of depth snapshots for each symbol
depths = {}


def on_message(ws, data):
    """Handle incoming WebSocket messages.

    Args:
        ws (WebSocketApp): WebSocket application instance.
        data (str): Message data received.
    """
    msg = json.loads(data)
    if 'MarketDepth' in msg:
        # Handle market depth updates
        symbol = msg['MarketDepth']['Symbol']
        if symbol not in depths:
            depths[symbol] = DepthSnapshot()
        depth = depths[symbol]
        depth.update(msg)
        depth.print_state()
    elif 'PublicTrade' in msg:
        # Handle public trade messages
        print("Public Trade:", msg)
    elif 'Symbols' in msg:
        # Handle symbols response
        print("Symbols:", msg)


# Start the WebSocket connection
print("Starting WebSocket connection...")
ws = WebSocketApp("ws://ws-feed.torobit.io:4444",
                  on_message=on_message,
                  on_open=on_open,
                  on_error=on_error)

ws.run_forever()


# ws.send(json.dumps({'Message': {'PublicTrades': {'Symbol': symbol}}}))
# depth = json.dumps({'Message': {'MarketDepth': {'Symbol': symbol}}})
# ws.send(depth)
# ws.send(json.dumps({'Message': {'PublicTrades': {'Symbol': symbol}}}))