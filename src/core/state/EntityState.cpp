#include "../../../include/core/state/EntityState.h"
#include <iostream>
#include <string>
// Constructor definition
EntityState::EntityState(const std::string& role, const std::string& state, int view, int sequence)
    : _role(role), _state(state), viewNumber(view), sequenceNumber(sequence) {
}

// Getters definition
std::string EntityState::getRole() const {
    return _role;
}

std::string EntityState::getState() const {
    return _state;
}

int EntityState::getSequenceNumber() const {
    return sequenceNumber;
}

int EntityState::getViewNumber() const {
    return viewNumber;
}

// Setters definition
void EntityState::setRole(const std::string& newRole) {
    _role = newRole;
}

void EntityState::setState(const std::string& newState) {
    _state = newState;
}

void EntityState::setSequenceNumber(int newSequenceNumber) {
    sequenceNumber = newSequenceNumber;
}

void EntityState::setViewNumber(int newViewNumber) {
    viewNumber = newViewNumber;
}

void EntityState::incrementSequenceNumber(){
    //std::cout << "incrementing sequence number" << std::endl;
    sequenceNumber+=1;
}
