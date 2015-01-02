// Copyright 2007-2012 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

using Lopezfoods.Scheduling;
using MassTransit;
using MassTransit.Logging;
using MassTransit.Scheduling;
using Quartz;

namespace ServiceBus.RuntimeServices
{
    public class CancelScheduledRecurringMessageConsumer :
        Consumes<ICancelScheduledRecurringMessage>.Context
    {
        static readonly ILog _log = Logger.Get<CancelScheduledRecurringMessageConsumer>();
        readonly IScheduler _scheduler;

        public CancelScheduledRecurringMessageConsumer(IScheduler scheduler)
        {
            _scheduler = scheduler;
        }

        public void Consume(IConsumeContext<ICancelScheduledRecurringMessage> context)
        {
            bool unscheduledJob = _scheduler.UnscheduleJob(new TriggerKey(context.Message.ScheduleId,context.Message.ScheduleGroup));

            if (_log.IsDebugEnabled)
            {
                if (unscheduledJob)
                {
                    _log.DebugFormat("CancelRecurringScheduledMessage: {0} at {1}", context.Message,
                        context.Message.Timestamp);
                }
                else
                    _log.DebugFormat("CancelRecurringScheduledMessage: no message found {0}", context.Message);
            }
        }
    }
}