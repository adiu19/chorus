#!/bin/bash
# setup-chorus-tmux.sh

SESSION="chorus"
PROJECT_DIR="$HOME/Documents/code/chorus"

# Kill existing session if exists
tmux kill-session -t $SESSION 2>/dev/null

# Create new session with first pane
tmux new-session -d -s $SESSION -c "$PROJECT_DIR"

# Create a 2x3 grid layout (6 panes: 5 nodes + 1 shell)
# First split horizontally into top and bottom
tmux split-window -v -t $SESSION:0 -c "$PROJECT_DIR"

# Split top row into 3 panes
tmux select-pane -t $SESSION:0.0
tmux split-window -h -c "$PROJECT_DIR"
tmux split-window -h -c "$PROJECT_DIR"

# Split bottom row into 3 panes
tmux select-pane -t $SESSION:0.3
tmux split-window -h -c "$PROJECT_DIR"
tmux split-window -h -c "$PROJECT_DIR"

# Layout:
# Pane 0 (top-left)    │ Pane 1 (top-mid)    │ Pane 2 (top-right)
# Pane 3 (bot-left)    │ Pane 4 (bot-mid)    │ Pane 5 (bot-right)

# Run nodes in first 5 panes
tmux send-keys -t $SESSION:0.0 'make run-node1' C-m
tmux send-keys -t $SESSION:0.1 'make run-node2' C-m
tmux send-keys -t $SESSION:0.2 'make run-node3' C-m
tmux send-keys -t $SESSION:0.3 'make run-node4' C-m
tmux send-keys -t $SESSION:0.4 'make run-node5' C-m

# Pane 5 is your command shell
tmux select-pane -t $SESSION:0.5

# Attach to session
tmux attach -t $SESSION
