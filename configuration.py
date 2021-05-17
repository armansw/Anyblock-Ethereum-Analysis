
class Config:

    # anyblock variables
    ANY_SERVER = "sql.anyblock.net"
    ANY_PORT = 5432
    ANY_USER = "9aa57491-8192-4799-82d7-c6e848db5fda"
    ANY_PASSWORD = "81ea7118-5377-4448-9f88-ff2a95dde4d8"
    ANY_DB = "ethereum_ethereum_mainnet"

    # local and server table name
    DB_NAME = "balance"

    # local postgresql variables
    LOCAL_SERVER = "localhost"
    LOCAL_PORT = 5432
    LOCAL_USER = "jie"
    LOCAL_PASSWORD = "geng"
    LOCAL_DB = "anyblock"

    # local sqlite file name
    DB_FILE = "cache.sqlite"
    TABLE_LOG = "logs"
    TABLE_ADDRESS = "addresses"
    TABLE_HISTORY = "history"

    # time seconds between begin and end to load once
    TIME_GAP = 300  # 5 minutes

    # The cached balance unit is Szabo: 1e-6
    # The server balance unit is wei: 1e-18
    # So we multiply 1e-12 to the server balance
    UNIT_TRANS = 1e-12

    # In the history table, the balance unit is Eth: 1.0
    # So we multiply 1e-6 to the cache balance
    UNIT_CALC = 1e-6

    # local last time stamp file
    STATUS_CACHE = "status.cache"
    STATUS_CALC = "status.calc"

    # threshold days to decide LTH or STH
    WALLET_THRESHOLD = 155

    # calculation chunk size
    CALC_CHUNK_SIZE = 1000

    # Calculation Stage 2) window duration 180
    WINDOW_SIZE = 178
