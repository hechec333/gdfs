package raft

// one anothor way to implement local read

// 1. make sure current leader has commit at lease one log
// 2. save current commitindex as readindex
// 3. make sure current raft group has not more newer leader
// 4. wait statemachine apply-index >= readindex
// 5. read local statemachine
