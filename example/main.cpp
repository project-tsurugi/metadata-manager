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
 *	@file	main.cpp
 *  @brief  sample code main
 */

#include <iostream>
#include "oltp_receiver.h"

#include "manager/message/message.h"
#include "manager/message/message_broker.h"

int main()
{
    manager::message::CreateTableMessage ct_msg_0{0};
    manager::message::CreateTableMessage ct_msg_1{1};
    manager::message::CreateTableMessage ct_msg_2{2};
    manager::message::CreateTableMessage ct_msg_3{3};

    manager::message::MessageBroker broker;
    manager::message::OltpReceiver oltp_receiver;
    ct_msg_0.set_receiver(&oltp_receiver);
    ct_msg_1.set_receiver(&oltp_receiver);
    ct_msg_2.set_receiver(&oltp_receiver);
    ct_msg_3.set_receiver(&oltp_receiver);

    manager::message::Status status = broker.send_message(&ct_msg_0);
    std::cout << "primary error code:" << (int)status.get_error_code()
              << ",secondary error code:" << status.get_sub_error_code() << std::endl;

    status = broker.send_message(&ct_msg_1);
    std::cout << "primary error code:"<< (int)status.get_error_code()
              << ",secondary error code:" << status.get_sub_error_code() << std::endl;

    status = broker.send_message(&ct_msg_2);
    std::cout << "primary error code:" << (int)status.get_error_code()
              << ",secondary error code:" << status.get_sub_error_code() << std::endl;

    status = broker.send_message(&ct_msg_3);
    std::cout << "primary error code:" << (int)status.get_error_code()
              << ",secondary error code:" << status.get_sub_error_code() << std::endl;
}
