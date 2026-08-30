#include "../../../include/core/state/StateMachine.h"
#include <iostream>
#include <string>

void StateMachine::handleMessage(const Message* message, EntityState* context) {
    std::cout << "[StateMachine] Handling message: \n";

    
}

void StateMachine::transitionState(EntityState* context, const std::string& newState) {
    std::cout << "[StateMachine] Transitioning from ";

}

void StateMachine::updateSequence(EntityState* context) {
    std::cout << "[StateMachine] Sequence updated to: \n";
}

