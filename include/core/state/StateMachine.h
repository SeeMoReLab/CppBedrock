#pragma once

#include "EntityState.h"
#include "../events/Message.h"
#include <iostream>
#include <string>

class StateMachine {
public:
    void handleMessage(const Message* message, EntityState* context);

private:
    void transitionState(EntityState* context, const std::string& newState);
    void updateSequence(EntityState* context);
};
