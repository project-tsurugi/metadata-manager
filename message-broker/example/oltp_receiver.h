/*
 * Copyright 2020 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *	@file	oltp_receiver.h
 *	@brief  the oltp receiver class that handle message
 */

#ifndef OLTP_RECEIVER_H
#define OLTP_RECEIVER_H

#include "manager/message/receiver.h"
#include "manager/message/message.h"
#include "manager/message/status.h"

namespace manager::message
{
    class OltpReceiver : public Receiver
    {
        public:
            Status receive_message(Message *message);
    };

}; // namespace manager::message

#endif // OLTP_RECEIVER_H
