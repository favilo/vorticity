default: (test "echo")

test target *OPTIONS:
    cargo build
    ../maelstrom/maelstrom test -w {{target}} --bin target/debug/{{target}} {{OPTIONS}}

all: (test "echo" "--node-count 1 --time-limit 10") \
        (test "unique-ids" "--node-count 3 --time-limit 10 --rate 1000 --availability total --nemesis partition")
