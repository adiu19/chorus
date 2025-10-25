#!/bin/bash
# setup-chorus-tmux.sh

SESSION="chorus"

# Kill existing session if exists
tmux kill-session -t $SESSION 2>/dev/null

# Create new session with first pane
tmux new-session -d -s $SESSION

# Create a clean 2x2 grid layout
# Split horizontally (top and bottom)
tmux split-window -v -t $SESSION:0

# Split left side vertically (top-left and bottom-left)
tmux select-pane -t $SESSION:0.0
tmux split-window -h

# Split right side vertically (top-right and bottom-right)
tmux select-pane -t $SESSION:0.2
tmux split-window -h

# Now we have a 2x2 grid:
# Pane 0 (top-left), Pane 1 (top-right)
# Pane 2 (bottom-left), Pane 3 (bottom-right)

# Send commands to each pane
tmux send-keys -t $SESSION:0.0 'docker logs -f chorus-node1' C-m
tmux send-keys -t $SESSION:0.1 'docker logs -f chorus-node2' C-m
tmux send-keys -t $SESSION:0.2 'docker logs -f chorus-node3' C-m
tmux send-keys -t $SESSION:0.3 'cd ~/Documents/code/chorus' C-m

# Select the command pane (bottom-right)
tmux select-pane -t $SESSION:0.3

# Attach to session
tmux attach -t $SESSION
