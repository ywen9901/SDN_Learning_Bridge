/*
 * Copyright 2022-present Open Networking Foundation
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
 */
package nctu.winlab.bridge;

import org.onlab.packet.Ethernet;
import org.onlab.packet.MacAddress;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import java.util.Dictionary;
import java.util.Map;
import java.util.Properties;

import static org.onlab.util.Tools.get;

/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true,
           service = {SomeInterface.class},
           property = {
               "someProperty=Some Default String Value",
           })
public class AppComponent implements SomeInterface {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String someProperty;

    private static final String MSG_ADD_TO_MAC_TABLE = "Add an enty to the port table of {}. MAC address: {} => Port: {}.";
    private static final String MSG_DST_MAC_MISSED = "MAC address {} is missed on {}. Flood the packet.";
    private static final String MSG_DST_MAC_MATCHED = "MAC address {} is matched on {}. Install a flow rule.";

    private static final int PRIORITY = 30;
    private static final int TIMEOUT_SEC = 30;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ComponentConfigService cfgService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowObjectiveService flowObjectiveService;

    private ApplicationId appId;
    private PacketProcessor processor;
    protected Map<DeviceId, Map<MacAddress, PortNumber>> macTables = Maps.newConcurrentMap();

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("nctu.winlab.bridge");
        log.info("Started");
        
        processor = new ForwardingProcessor();
        packetService.addProcessor(processor, PacketProcessor.director(3));

        packetService.requestPackets(DefaultTrafficSelector.builder().matchEthType(Ethernet.TYPE_IPV4).build(), PacketPriority.REACTIVE, appId);
        packetService.requestPackets(DefaultTrafficSelector.builder().matchEthType(Ethernet.TYPE_ARP).build(), PacketPriority.REACTIVE, appId);
    }

    @Deactivate
    protected void deactivate() {
        flowRuleService.removeFlowRulesById(appId);
        packetService.removeProcessor(processor);
        processor = null;
        log.info("Stopped");
    }
    
    private class ForwardingProcessor implements PacketProcessor {
        @Override
        public void process(PacketContext context) {
            initMacTable(context.inPacket().receivedFrom());
            forward(context);
        }

        public void initMacTable(ConnectPoint point) {
            macTables.putIfAbsent(point.deviceId(), Maps.newConcurrentMap());
        }

        public void flood(PacketContext context) {
            context.treatmentBuilder().setOutput(PortNumber.FLOOD);
            context.send();
        }

        public void forward(PacketContext context) {
            InboundPacket pkt = context.inPacket();
            Short type = pkt.parsed().getEtherType();
            if(type != Ethernet.TYPE_ARP && type != Ethernet.TYPE_IPV4) {
                return;
            }

            ConnectPoint point = context.inPacket().receivedFrom();
            Map<MacAddress, PortNumber> macTable = macTables.get(point.deviceId());

            MacAddress srcMac = context.inPacket().parsed().getSourceMAC();
            MacAddress dstMac = context.inPacket().parsed().getDestinationMAC();

            macTable.put(srcMac, point.port());
            log.info(MSG_ADD_TO_MAC_TABLE, point.deviceId(), srcMac, point.port());
            PortNumber outPort = macTable.get(dstMac);

            if(outPort == null) {
                log.info(MSG_DST_MAC_MISSED, dstMac, point.deviceId());
                flood(context);
            } else {
                log.info(MSG_DST_MAC_MATCHED, dstMac, point.deviceId());
                context.treatmentBuilder().setOutput(outPort);
                FlowRule rule = DefaultFlowRule.builder()
                    .withSelector(DefaultTrafficSelector.builder().matchEthDst(dstMac).build())
                    .withTreatment(DefaultTrafficTreatment.builder().setOutput(outPort).build())
                    .forDevice(point.deviceId())
                    .withPriority(PRIORITY)
                    .makeTemporary(TIMEOUT_SEC)
                    .fromApp(appId)
                    .build();
                
                flowRuleService.applyFlowRules(rule);
                context.send();
            }
        }
    }

    @Modified
    public void modified(ComponentContext context) {
        Dictionary<?, ?> properties = context != null ? context.getProperties() : new Properties();
        if (context != null) {
            someProperty = get(properties, "someProperty");
        }
        log.info("Reconfigured");
    }

    @Override
    public void someMethod() {
        log.info("Invoked");
    }

}
