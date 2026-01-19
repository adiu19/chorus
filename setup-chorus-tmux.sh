#!/bin/bash
# setup-chorus-tmux.sh

SESSION="chorus"
NODE_DIR="$HOME/Documents/code/chorus/node"

# Kill existing session if exists
tmux kill-session -t $SESSION 2>/dev/null

# Create new session with first pane
tmux new-session -d -s $SESSION -c "$NODE_DIR"

# Create a 2x2 grid layout
tmux split-window -v -t $SESSION:0 -c "$NODE_DIR"
tmux select-pane -t $SESSION:0.0
tmux split-window -h -c "$NODE_DIR"
tmux select-pane -t $SESSION:0.2
tmux split-window -h -c "$NODE_DIR"

# Layout:
# Pane 0 (top-left)     │ Pane 1 (top-right)
# Pane 2 (bottom-left)  │ Pane 3 (bottom-right)

# Run nodes in first 3 panes
tmux send-keys -t $SESSION:0.0 'make run-node1' C-m
tmux send-keys -t $SESSION:0.1 'make run-node2' C-m
tmux send-keys -t $SESSION:0.2 'make run-node3' C-m

# Pane 3 is your command shell
tmux select-pane -t $SESSION:0.3

# Attach to session
tmux attach -t $SESSION
