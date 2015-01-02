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

using System;
using System.Collections.Generic;
using System.Globalization;
using MassTransit;
using MassTransit.Context;
using MassTransit.Logging;
using Newtonsoft.Json;
using Quartz;
using ServiceBus.RuntimeServices;

namespace Lopezfoods.Scheduling
{
    public class ScheduledRecurringMessageJob : IJob
    {
        static readonly ILog _log = Logger.Get<ScheduledRecurringMessageJob>();

        readonly IServiceBus _bus;

        public ScheduledRecurringMessageJob(IServiceBus bus)
        {
            _bus = bus;
        }

        public string Destination { get; set; }

        public string ExpirationTime { get; set; }
        public string ResponseAddress { get; set; }
        public string FaultAddress { get; set; }
        public string Body { get; set; }

        public string MessageId { get; set; }
        public string MessageType { get; set; }
        public string ContentType { get; set; }
        public string RequestId { get; set; }
        public string ConversationId { get; set; }
        public string CorrelationId { get; set; }
        public string Network { get; set; }
        public int RetryCount { get; set; }
        public string HeadersAsJson { get; set; }

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                JobExecutionContext = context;
                var destinationAddress = new Uri(Destination);
                Uri sourceAddress = _bus.Endpoint.Address.Uri;

                IEndpoint endpoint = _bus.GetEndpoint(destinationAddress);

                ISendContext messageContext = CreateMessageContext(sourceAddress, destinationAddress);

               

                endpoint.OutboundTransport.Send(messageContext);
            }
            catch (Exception ex)
            {
                string message = string.Format(CultureInfo.InvariantCulture,
                    "An exception occurred sending message {0} to {1}", MessageType, Destination);
                _log.Error(message, ex);

                throw new JobExecutionException(message, ex);
            }
        }

        public IJobExecutionContext JobExecutionContext { get; set; }


        ISendContext CreateMessageContext(Uri sourceAddress, Uri destinationAddress)
        {
            var context = new ScheduledRecurringMessageContext(Body);

            context.SetDestinationAddress(destinationAddress);
            context.SetSourceAddress(sourceAddress);
            context.SetResponseAddress(ToUri(ResponseAddress));
            context.SetFaultAddress(ToUri(FaultAddress));

            SetHeaders(context);
            SetBodyHeaders(context);
            context.SetMessageId(MessageId);
            context.SetRequestId(RequestId);
            context.SetConversationId(ConversationId);
            context.SetCorrelationId(CorrelationId);

            if (!string.IsNullOrEmpty(ExpirationTime))
                context.SetExpirationTime(DateTime.Parse(ExpirationTime));

            context.SetNetwork(Network);
            context.SetRetryCount(RetryCount);
            context.SetContentType(ContentType);

            return context;
        }

        void SetHeaders(ScheduledRecurringMessageContext context)
        {
            if (string.IsNullOrEmpty(HeadersAsJson))
                return;

            var headers = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(HeadersAsJson);
            context.SetHeaders(headers);
        }


        void SetBodyHeaders(ScheduledRecurringMessageContext context)
        {

            var headers = new Dictionary<string, string>();

            headers["FireTimeUtc"] = JobExecutionContext.FireTimeUtc.HasValue ? JobExecutionContext.FireTimeUtc.ToString() : null;
            headers["NextFireTimeUtc"] = JobExecutionContext.NextFireTimeUtc.HasValue ? JobExecutionContext.NextFireTimeUtc.ToString() : null;
            headers["Recovering"] = JobExecutionContext.Recovering.ToString();
            headers["ScheduledFireTimeUtc"] = JobExecutionContext.ScheduledFireTimeUtc.HasValue ? JobExecutionContext.ScheduledFireTimeUtc.ToString() : null;
            headers["PreviousFireTimeUtc"] = JobExecutionContext.PreviousFireTimeUtc.HasValue ? JobExecutionContext.PreviousFireTimeUtc.ToString() : null;


            context.SetBodyHeaders(headers);
        }

        static Uri ToUri(string s)
        {
            if (string.IsNullOrEmpty(s))
                return null;

            return new Uri(s);
        }
    }
}