import time
import struct
import lz4.block

class FastReader:
    """
    FastReader reads and decompresses LZ4 compressed binary data from a file.
    It iterates over the decompressed data to yield structured messages.
    """
    def __init__(self, fn):
        self.f = open(fn, 'rb')
        self.ulen = struct.unpack('=i', self.f.read(4))[0]  # Uncompressed length
        self.offset = 0
        self.datalen = 0

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            # If all data has been read, decompress the next block
            if self.offset >= self.datalen:
                lenbytes = self.f.read(4)
                if len(lenbytes) < 4:
                    raise StopIteration
                clen = struct.unpack('=i', lenbytes)[0]
                if clen == 0:
                    raise StopIteration
                # Decompress the next chunk of data
                self.data = lz4.block.decompress(self.f.read(clen), self.ulen)
                self.datalen = len(self.data)
                self.offset = 0

            # Read the message header
            header = struct.unpack_from('=hhq', self.data, self.offset)

            # Define message structure based on header type
            if header[0] == 0:
                msg = struct.unpack_from('=hhqqqB', self.data, self.offset)
            elif header[0] == 1:
                msg = struct.unpack_from('=hhqqqqB', self.data, self.offset)
            else:
                msg = None

            self.offset += header[1]

            # Return the message if valid
            if msg is not None:
                print("Received message:", msg)
                return msg

class DepthSnapshot:
    """
    DepthSnapshot maintains order book bids and asks, and updates based on incoming messages.
    """
    def __init__(self):
        self.bids = {}
        self.asks = {}

    def update(self, msg):
        """
        Updates the order book with a new message. Clears bids and asks if it's a snapshot update.
        """
        if msg[5] & 4:  # Check if it's a snapshot
            self.bids.clear()
            self.asks.clear()
            print('Snapshot received.')

        # Determine if the update is for bids or asks
        items = self.bids if msg[5] & 1 else self.asks
        price = msg[3] / 10**8
        volume = msg[4] / 10**8

        # Update the order book
        if msg[4] > 0:
            items[price] = volume
        else:
            items.pop(price, None)

    def printstate(self):
        """
        Prints the current state of the order book including best bid and ask prices.
        """
        best_ask = min(self.asks) if self.asks else None
        best_bid = max(self.bids) if self.bids else None

        print('Bids count:', len(self.bids))
        print('Asks count:', len(self.asks))
        print('Best bid:', best_bid)
        print('Best ask:', best_ask)

class TradeProcessor:
    """
    TradeProcessor handles trade messages, storing trades with price and volume.
    """
    def __init__(self):
        self.trades = []

    def update(self, msg):
        """
        Updates the trade list with new trade data from messages.
        """
        price = msg[3] / 10**8
        volume = msg[4] / 10**8
        self.trades.append((msg[2], price, volume))

    def printstate(self):
        """
        Prints the current state of trade data, including the latest trade.
        """
        print('Trades count:', len(self.trades))
        if self.trades:
            print('Last trade:', self.trades[-1])

def process_messages(file_path):
    """
    Processes messages from the given file path, updating depth and trade processors.
    """
    start = time.time()
    count = 0

    depth = DepthSnapshot()
    trades = TradeProcessor()

    for msg in FastReader(file_path):
        if msg[0] == 0:
            depth.update(msg)
        elif msg[0] == 1:
            trades.update(msg)

        depth.printstate()
        trades.printstate()
        count += 1

    end = time.time()
    print("Elapsed time:", end - start)
    print("Message count:", count)

if __name__ == "__main__":
    file_path = 'E://lz4data//20230414//BTC-USD@COINBASE_20230414.bin.lz4'
    process_messages(file_path)
