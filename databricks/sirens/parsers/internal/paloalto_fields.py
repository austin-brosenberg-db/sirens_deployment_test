from .common import get_spark_type_for_data_type

__paloalto_log_fields__ = {
    "access_point_name": {"description": "Indicates the access point name, which is a reference to a Packet "
                                         "Data Network Data Gateway (PGW)/ Gateway GPRS Support Node in a "
                                         "mobile network."},
    "action": {"description": "Identifies the action that the firewall took for the network traffic."},
    "action_source": {"description": "Specifies whether the action taken to allow or block an application "
                                     "was defined in the application or in policy."},
    "admin_user": {"description": "Username of the administrator performing the configuration."},
    "admin_user_info_domain": {"description": "Domain to which the admin user belongs."},
    "admin_user_info_name": {"description": "Name of the user who created the configuration change."},
    "admin_user_info_uuid": {"description": "The admin user's unique ID."},
    "agent_content_version": {
        "description": "Version of the agent content that is installed on the endpoint."},
    "agent_data_collection_status": {"description": "Indicates whether data related to another product (for "
                                                    "example, EDR) is being collected by the agent."},
    "agent_id": {"description": "Unique identifier for the agent at the endpoint."},
    "agent_isolation_status": {"description": "Indicates whether the agent is isolated. Usually, agents are "
                                              "isolated if they have been compromised."},
    "agent_protection_status": {"description": "The protection status set for the endpoint."},
    "agent_version": {"description": "Version of the agent at the endpoint."},
    "app": {"description": "Application associated with the network traffic."},
    "app_category": {"description": "Identifies the high-level family of the application."},
    "app_sub_category": {"description": "Identifies the application's subcategory. The subcategory is "
                                        "related to the application's category, which is identified in "
                                        "category_of_app."},
    "association_end_reason": {"description": "The reason the session terminated. If the termination had "
                                              "multiple reasons, only the highest priority reason is "
                                              "identified here."},
    "attempted_gateways": {"description": "String of all gateways that were available and attempted for the "
                                          "client location. Contains gateway name, ssl response time, "
                                          "and priority, separated by a semicolon."},
    "auth_completion_time": {"description": "Time when the authentication was completed. This string "
                                            "contains a timestamp value that is the number of microseconds "
                                            "since the Unix epoch.",
                             "type": "pa_timestamp"},
    "auth_description": {"description": "Additional authentication information."},
    "auth_event_name": {"description": "The authentication event that caused the firewall to create this "
                                       "log record."},
    "auth_factor_num": {"description": "Indicates the use of primary authentication (1) or additional "
                                       "factors (2, 3)."},
    "auth_method": {"description": "Authentication method used for the GlobalProtect connection."},
    "auth_policy": {"description": "Policy invoked for authentication before allowing access to a protected "
                                   "resource."},
    "auth_proto": {"description": "Indicates the authentication protocol used by the server. For example, "
                                  "PEAP with GTC."},
    "auth_server_profile": {"description": "Authentication server used for authentication."},
    "authenticated_user_info_domain": {"description": "Domain to which the user who is being authenticated "
                                                      "belongs."},
    "authenticated_user_info_name": {"description": "Name of the user who is being authenticated."},
    "authenticated_user_info_uuid": {"description": "Unique identifier assigned to the user who is being "
                                                    "authenticated."},
    "bytes_received": {"description": "Number of bytes in the server-to-client network traffic."},
    "bytes_sent": {"description": "Number of bytes in the client-to-server network traffic."},
    "bytes_total": {"description": "Number of total bytes (transmit and receive)."},
    "cert_flags": {"description": "Internal use only bit field containing raw decryption information as "
                                  "generated at the firewall. The information in this bit field is "
                                  "reflected in other decryption log fields."},
    "cert_serial": {"description": "The certificate's serial number."},
    "certificate_size": {"description": "The size of the certificate."},
    "certificate_version": {"description": "The certificate's version number."},
    "chain_status": {"description": "The certificate chain verification status. Possible values are: "
                                    "Uninspected, Untrusted, Trusted, Incomplete."},
    "characteristics_of_app": {"description": "Identifies the behaviorial characteristic of the application "
                                              "associated with the network traffic."},
    "chunks_received": {"description": "The total number of SCTP data chunks in the server-to-client "
                                       "network traffic."},
    "chunks_sent": {"description": "The total number of SCTP data chunks in the client-to-server network "
                                   "traffic."},
    "chunks_total": {"description": "The total number of SCTP data chunks in the network traffic."},
    "client": {"description": "Client used by the administrator who is performing the configuration."},
    "client_to_firewall": {"description": "The direction of the SSL/TLS connection is from the client to "
                                          "the firewall."},
    "client_type": {"description": "Type of client used to complete authentication (such as authentication "
                                   "portal)."},
    "client_type_name": {"description": "Type of client used to complete authentication."},
    "cloud": {"description": "FQDN of either the appliance (private) or the cloud (public) from where the "
                             "file was uploaded for analysis."},
    "cloud_hostname": {"description": "The hostname in which the VM-series firewall is running."},
    "cloud_reportid": {"description": "Unique 32 character ID for a file scanned by the DLP cloud service "
                                      "sent by a firewall running PAN-OS 10.2.0. The same Cloud Report ID "
                                      "is displayed for a file the DLP cloud service has already scanned "
                                      "and generated a Cloud Report ID for."},
    "cn": {"description": "The common name found on the certificate's domain name."},
    "cn_len": {
        "description": "The length of the common name found on the certificate's domain name before "
                       "truncation (if any).",
        "type": "int",
    },
    "config_version": {"description": "Config version converted to string represented as "
                                      "major.minor.patch.build in value and as hex in id."},
    "connect_method": {"description": "Identifies how the GlobalProtect app connected to the the Gateway. "
                                      "For example, on-demand or user-logon."},
    "connection_error": {"description": "Error information for unsuccessful connection."},
    "connection_error_id": {
        "description": "Enumeration integer assigned to the connection_error field value."},
    "container_id": {"description": "Unknown field. No information is available at this time."},
    "container_of_app": {"description": "Identifies the managing application or parent of the application "
                                        "associated with this network traffic."},
    "content_type": {"description": "Content type of the HTTP response data."},
    "content_version": {"description": "Version of the content on the firewall."},
    "count_of_repeats": {"description": "Number of sessions with same Source IP, Destination IP, "
                                        "Application, and Content/Threat Type seen for the summary interval "
                                        "or number of times the HIP profile matched."},
    "cpadding": {"description": "For internal use only."},
    "customer_id": {"description": "The ID that uniquely identifies the Cortex Data Lake instance which "
                                   "received this log record."},
    "dest_device_category": {"description": "Category of the device to which the session was directed."},
    "dest_device_class": {"description": "Destination device class."},
    "dest_device_host": {"description": "Hostname of the device to which the session was directed."},
    "dest_device_mac": {"description": "MAC Address of the device to which the session was directed."},
    "dest_device_model": {"description": "Model of the device to which the session was directed."},
    "dest_device_os": {"description": "Destination device OS type."},
    "dest_device_os_family": {"description": "OS family of the device to which the session was directed."},
    "dest_device_os_version": {"description": "OS version of the device to which the session was directed."},
    "dest_device_profile": {"description": "Profile of the device to which the session was directed."},
    "dest_device_vendor": {"description": "Vendor of the device to which the session was directed."},
    "dest_dynamic_address_group": {"description": "The dynamic address group that Device-ID identifies as "
                                                  "the destination for the traffic."},
    "dest_edl": {"description": "The name of the external dynamic list that contains the destination IP "
                                "address of the traffic."},
    "dest_ip": {"type": "ip", "description": "Original destination IP address. CEF fields: dst or c6a3"},
    "dest_location": {"description": "Destination country or internal region for private addresses."},
    "dest_port": {"type": "int", "description": "Network traffic's destination port. If this value is 0, "
                                                "then the app is using its standard port."},
    "dest_user": {"description": "The username to which the network traffic was destined."},
    "dest_user_info_domain": {"description": "Domain to which the Destination User belongs."},
    "dest_user_info_name": {"description": "The Destination User. That is, the username to which the "
                                           "network traffic was destined."},
    "dest_user_info_uuid": {"description": "Unique identifier assigned to the Destination User."},
    "dest_uuid": {"description": "Identifies the destination universal unique identifier for a guest "
                                 "virtual machine in the VMware NSX environment."},
    "device_group": {"description": "The ID and the name of the device group the firewall is in."},
    "dg_hier_level_1": {"description": "A sequence of identification numbers that indicate the device "
                                       "group’s location within a device group hierarchy."},
    "dg_hier_level_2": {"description": "A sequence of identification numbers that indicate the device "
                                       "group’s location within a device group hierarchy."},
    "dg_hier_level_3": {"description": "A sequence of identification numbers that indicate the device "
                                       "group’s location within a device group hierarchy."},
    "dg_hier_level_4": {"description": "A sequence of identification numbers that indicate the device "
                                       "group’s location within a device group hierarchy."},
    "diam_app_id": {"description": "The IANA ID assigned to the Diameter application associated with this "
                                   "network traffic."},
    "diam_avp_code": {"description": "The AVP code used by the Diameter application associated with this "
                                     "network traffic."},
    "diam_cmd_code": {"description": "The Diameter command code used by this network traffic."},
    "direction_of_attack": {"description": "Indicates the direction of the attack."},
    "dlp_version_flag": {"description": "Indicates whether these are old or new data filtering logs."},
    "domain": {"description": "The subject common name; that is, the name of the server that the "
                              "certificate protects."},
    "domain_edl": {"description": "Domain External Dynamic List. That is, the name of the external dynamic "
                                  "list that contains the destination domain of the traffic."},
    "dynusergroup_name": {"description": "Dynamic user group of the user who initiated the network "
                                         "connection."},
    "elliptic_curve": {"description": "The elliptic cryptography curve that the client and server negotiate "
                                      "and use for connections that use ECDHE cipher suites."},
    "endpoint_cpu_architecture": {"description": "The architecture of the OS type that the endpoint is "
                                                 "running."},
    "endpoint_device_domain": {"description": "Domain to which the endpoint belongs."},
    "endpoint_device_name": {"description": "Hostname of the endpoint on which the event was logged."},
    "endpoint_gp_version": {"description": "GlobalProtect client version number."},
    "endpoint_ip": {"type": "ip", "description": "IP address of the source of the event."},
    "endpoint_is_vdi": {"description": "Indicates whether the endpoint is a virtual desktop infrastructure "
                                       "(VDI). 0—The endpoint is not a VDI, 1—The endpoint is a VDI."},
    "endpoint_os_type": {"description": "The operating system on which the endpoint is running."},
    "endpoint_os_version": {"description": "The version of the operating system running on the endpoint."},
    "endpoint_serial_number": {
        "description": "Serial number of the host on which GlobalProtect is installed."},
    "endpoint_tz_offset": {"description": "Effective endpoint time zone offset from UTC, in minutes."},
    "endpoint_user_domain": {"description": "Domain of the user who was logged into the endpoint at the "
                                            "time of the system event."},
    "endpoint_user_name": {"description": "The name of the user logged into the endpoint at the time of the "
                                          "system event."},
    "endpoint_user_uuid": {"description": "The endpoint user's unique ID."},
    "ep_assoc_id": {"description": "The ID assigned to the endpoint association used for the SCTP network "
                                   "traffic."},
    "error_index": {"description": "The elliptic cryptography curve that the client and server negotiate "
                                   "and use for connections that use ECDHE cipher suites."},
    "error_message": {"description": "The error message content."},
    "event_client_ip": {"type": "ip", "description": "Hostname or IP address of the client."},
    "event_code": {"description": "The SCTP event notification code set for this message."},
    "event_component": {
        "description": "The component associated with the event. For example, the object from a firewall."},
    "event_description": {
        "description": "Description of the system event. If the source is a firewall, this is opaque. If "
                       "the source is TMS, this is the msgTextEn field."},
    "event_detail": {
        "description": "Identifies the firewall's configuration prior to and immediately after the "
                       "configuration change."},
    "event_id": {"description": "The event's unique identifier."},
    "event_name": {"description": "Name of the system event."},
    "event_path": {"description": "The path of the configuration command issued."},
    "event_result": {"description": "Result of the configuration action."},
    "event_time": {
        "description": "Time when the log was generated on the firewall's data plane. This string contains "
                       "a timestamp value that is the number of microseconds since the Unix epoch.",
        "type": "pa_timestamp"},
    "event_type": {"description": "The SCTP event notification type set for this message."},
    "file_name": {"description": "The name of the file that is blocked."},
    "file_sha_256": {"description": "The binary hash (SHA256) of the file."},
    "file_type": {"description": "The type of the file sent for virus analysis."},
    "file_url": {"description": "File URL."},
    "fingerprint": {"description": "A hash of the certificate in x509 binary format."},
    "firewall_to_client": {
        "description": "The direction of the SSL/TLS connection is from the firewall to the client."},
    "from_zone": {"description": "The networking zone from which the traffic originated."},
    "gateway": {"description": "Selected Gateway for the connection."},
    "gateway_priority": {"description": "Priority of gateway, retrieved from portal configuration."},
    "gateway_selection_type": {"description": "Gateway Selection Method i.e automatic, preferred or manual."},
    "gp_host_id": {"description": "A unique ID that GlobalProtect assigns to identify the host."},
    "gpg_location": {"description": "Location of the Global Protect Gateway."},
    "ha_session_owner": {"description": "Name of cluster member in which session failed over from."},
    "hip_match_name": {"description": "Name of the HIP object or profile."},
    "hip_match_type": {
        "description": "Identifies whether the hip field represents a HIP object or a HIP profile."},
    "host_id": {"description": "A unique ID that GlobalProtect assigns to identify the host."},
    "http2_connection": {
        "description": "Parent session ID for an HTTP/2 connection. If the traffic is not using HTTP/2, "
                       "this field is set to 0."},
    "http_headers": {"description": "The HTTP headers used in the web request."},
    "http_method": {"description": "Describes the HTTP Method used in the web request."},
    "inbound_if": {"description": "Interface from which the network traffic was sourced."},
    "inbound_if_details_port": {
        "type": "int",
        "description": "Hardware port or socket from which the network traffic was sourced."},
    "inbound_if_details_slot": {"description": "Interface slot from which the network traffic was sourced."},
    "inbound_if_details_type": {
        "description": "The type of interface from which the network traffic was sourced."},
    "inbound_if_details_unit": {"description": "Internal use."},
    "inline_ml_verdict": {
        "description": "A verdict that identifies the nature of the threat based on the Inline ML model "
                       "used to analyze the webpage."},
    "ip_subnet_range": {"description": "IP subnet range."},
    "is_captive_portal": {
        "description": "Indicates if user information for the session was captured through Captive Portal."},
    "is_cert_ECDSA": {"description": "The certificate key exchange algorithm used for the session is ECDSA."},
    "is_cert_RSA": {"description": "The certificate key exchange algorithm used for the session is RSA."},
    "is_cert_cn_truncated": {
        "description": "Indicates whether the common name found on the certificate has been truncated due "
                       "to buffer limits."},
    "is_client_to_server": {"description": "Indicates if direction of traffic is from client to server."},
    "is_container": {"description": "Indicates if the session is a container page access (Container Page)."},
    "is_decrypt_mirror": {
        "description": "Indicates whether decrypted traffic was sent out in clear text through a mirror port."
    },
    "is_decrypted": {"description": "Flag that indicates that the session is decrypted."},
    "is_decrypted_payload_fwded": {"description": "Unknown field. No information is available at this time."},
    "is_decryption_log": {"description": "Unknown field. No information is available at this time."},
    "is_dup_log": {
        "description": "Indicates whether this log data is available in multiple locations, such as from "
                       "the Logging Service and also from an on-premise log collector."},
    "is_duplicate_user": {"description": "Indicates whether duplicate users were found in a user group."},
    "is_encrypted": {"description": "Flag that indicates that the session is encrypted."},
    "is_exported": {
        "description": "Indicates if this log was exported from the firewall using the firewall's log "
                       "export function."},
    "is_forwarded": {"description": "Indicates if the log is being forwarded."},
    "is_ipv6": {"description": "Indicates whether IPV6 was used for the session."},
    "is_issuer_cn_truncated": {
        "description": "Indicates whether the common name used by the certificate's issuer has been "
                       "truncated due to buffer limits."},
    "is_l7_inspection_b4_session": {
        "description": "Unknown field. No information is available at this time."},
    "is_mptcp_on": {
        "description": "Indicates whether the option is enabled on the next-generation firewall that allows "
                       "a client to use multiple paths to connect to a destination host."},
    "is_nat": {
        "description": "Indicates if the firewall is performing network address translation (NAT) for the "
                       "logged traffic."},
    "is_non_std_dest_port": {"description": "Indicates if the destination port is non-standard."},
    "is_offloaded": {
        "description": "Indicates whether the traffic flow is offloaded to hardware before the packets "
                       "enter Linux kernel on VM/CN series."},
    "is_packet_capture": {"description": "Indicates whether the session has a packet capture (PCAP)."},
    "is_phishing": {"description": "Indicates whether enterprise credentials were submitted by an end user."},
    "is_prisma_branch": {
        "description": "If set to 1, the log was generated on a cloud-based firewall. If 0, the firewall "
                       "was running on-premise."},
    "is_prisma_mobile": {
        "description": "If set to 1, the log record was generated using a cloud-based GlobalProtect "
                       "instance. If 0, GlobalProtect was hosted on-premise."},
    "is_proxy": {"description": "Indicates whether the SSL session is decrypted (SSL Proxy)."},
    "is_recon_excluded": {
        "description": "Indicates whether source for the flow is on the firewall allow list and not subject "
                       "to recon protection."},
    "is_resume_session": {
        "description": "Indicates that the decryption session was previously interrupted and is now resuming."
    },
    "is_root_cn_truncated": {
        "description": "Indicates whether the common name used for the root CA has been truncated due to "
                       "buffer limits."},
    "is_saas_app": {
        "description": "Internal use field. Indicates whether the application associated with this network "
                       "traffic is a SAAS application."},
    "is_server_to_client": {"description": "Indicates if direction of traffic is from server to client."},
    "is_sni_truncated": {
        "description": "Indicates whether the server name indication (SNI), which is the hostname of the "
                       "server that the client is trying to reach, has been truncated due to buffer "
                       "limits."},
    "is_source_x_fwded": {
        "description": "Indicates whether the X-Forwarded-For value from a proxy is in the source user field."
    },
    "is_sym_return": {
        "description": "Indicates whether symmetric return was used to forward traffic for this session."},
    "is_transaction": {
        "description": "Indicates whether the log corresponds to a transaction within an HTTP proxy session "
                       "(Proxy Transaction)."},
    "is_tunnel_inspected": {
        "description": "Indicates whether the payload for the outer tunnel was inspected."},
    "is_url_denied": {"description": "Indicates whether the session was denied due to a URL filtering rule."},
    "issuer_cn": {"description": "The name of the organization that verified the certificate’s contents."},
    "issuer_len": {
        "description": "The length of the issuer's common name before truncation (if any).",
        "type": "int",
    },
    "justification": {"description": "Justification string."},
    "link_change_count": {"description": "Number of times the app flapped in that session."},
    "link_switches": {"description": "Details of the links switches (up-to 4)."},
    "location": {"description": "Prisma Access Region/Location."},
    "log_category": {"description": "The log category."},
    "log_set": {
        "description": "Log forwarding profile name that was applied to the session. This name was defined "
                       "by the firewall's administrator."},
    "log_source": {
        "description": "Identifies the origin of the data. That is, the system that produced the data."},
    "log_source_id": {"type": "long",
                      "description": "ID that uniquely identifies the source of the log. If the source is a "
                                     "firewall, this is its serial number. If the source is TMS, "
                                     "this is the trapsId."},
    "log_source_name": {
        "description": "Name of the source of the log. If the source is a firewall, this is the device_name "
                       "value. If the source is TMS, this is either the customer or tenant name."},
    "log_source_tz_offset": {"description": "Time Zone offset from GMT of the source of the log."},
    "log_time": {"type": "pa_timestamp",
                 "description": "Time the log was received in Cortex Data Lake. This is populated by the "
                                "platform."},
    "log_type": {"description": "Identifies the log type."},
    "login_duration": {"description": "Duration for which the connected user was logged on."},
    "map_op_code": {
        "description": "Mobile Application Part (MAP) operation code used for this network traffic."},
    "mapping_data_source": {"description": "Source from which mapping information is collected."},
    "mapping_data_source_name": {
        "description": "Name of the source from which the mapping information was collected."},
    "mapping_data_source_sub_type": {
        "description": "Mechanism used to identify the IP/User mappings within a data source."},
    "mapping_data_source_type": {"description": "Source from which mapping information is collected."},
    "mapping_timeout": {"description": "Timeout interval after which the IP/User Mappings are cleared."},
    "mfa_auth_id": {
        "description": "Unique ID given across primary authentication and additional (multi-factor) "
                       "authentication."},
    "mfa_factor_type": {
        "description": "The vendor used to authenticate a user when multi-factor authentication is present."},
    "mfa_vendor": {"description": "Vendor providing additional factor authentication."},
    "mobile_area_code": {"description": "Area within a Public Land Mobile Network (PLMN)."},
    "mobile_base_station_code": {"description": "Base station within an area code."},
    "mobile_country_code": {"description": "Mobile country code of serving core network operator."},
    "mobile_ip": {"description": "IP address of a mobile subscriber allocated by a PGW/GGSN."},
    "mobile_network_code": {"description": "Mobile network code of serving core network operator."},
    "mobile_subscriber_isdn": {"description": "Service identity associated with the mobile subscriber."},
    "monitor_tag_imei": {
        "description": "A string used to group similar traffic together for logging and reporting. This "
                       "value is globally defined on the firewall by the administrator."},
    "nat_dest": {"type": "ip",
                 "description": "If destination NAT was performed, the post-NAT destination IP address."},
    "nat_dest_port": {"type": "int", "description": "Post-NAT destination port."},
    "nat_source": {"type": "ip",
                   "description": "If source NAT was performed, the post-NAT source IP address."},
    "nat_source_port": {"type": "int", "description": "Post-NAT source port."},
    "non_standard_dest_port": {
        "description": "Identifies the non-standard or unexpected port used by the application associated "
                       "with this session."},
    "normalize_user": {
        "description": "Normalized version of the username being authenticated (such as appending a domain "
                       "name to the username)."},
    "not_after": {"description": "Timestamp date after which the certificate is no longer valid."},
    "not_before": {"description": "Timestamp date before which the certificate is not yet valid."},
    "nssai_network_slice_differentiator": {
        "description": "Network Slice Differentiator (SD part of SNSSAI)."},
    "nssai_network_slice_type": {"description": "Network Slice Type (SST part of SNSSAI)."},
    "object": {"description": "Name of the object associated with the system event."},
    "opaque": {"description": "Additional information regarding the event."},
    "outbound_if": {"description": "Interface to which the network traffic was destined."},
    "outbound_if_details_port": {"type": "int",
                                 "description": "Hardware port or socket to which the network traffic was "
                                                "sent."},
    "outbound_if_details_slot": {"description": "Interface slot to which the network traffic was sent."},
    "outbound_if_details_type": {
        "description": "The type of interface to which the network traffic was sent."},
    "outbound_if_details_unit": {"description": "Internal use."},
    "packets_dropped_max_encap": {
        "description": "Number of packets the firewall dropped because the packet exceeded the maximum "
                       "number of encapsulation levels configured."},
    "packets_dropped_strict_check": {
        "description": "Number of packets the firewall dropped because the tunnel protocol header in the "
                       "packet failed to comply with the RFC for the tunnel protocol."},
    "packets_dropped_tunnel_frag": {
        "description": "Number of packets the firewall dropped because of fragmentation errors."},
    "packets_dropped_ukn_proto": {
        "description": "Number of packets the firewall dropped because the packet contains an unknown "
                       "protocol."},
    "packets_received": {"description": "Number of server-to-client packets for the session."},
    "packets_sent": {"description": "Number of client-to-server packets for the session."},
    "packets_total": {"description": "Number of total packets (transmit and receive) seen for the session."},
    "padding": {"description": "For internal use only."},
    "padding3": {"description": "For internal use only."},
    "parent_session_id": {"description": "ID of the session in which this network traffic was tunneled."},
    "parent_start_time": {
        # TODO: cast to long & convert into timestamp - check real type first
        "description": "Time that the parent session began. This string contains a timestamp value that is "
                       "the number of microseconds since the Unix epoch."},
    "partial_hash": {"description": "Machine learning partial hash."},
    "payload_protocol_id": {"description": "The associated Payload Protocol Identifier."},
    "pcap": {"description": "Packet that triggered the firewall to generate this log record."},
    "pcap_id": {
        "description": "Packet capture ID. Used to correlate threat pcap files with extended pcaps taken as "
                       "a part of the session flow."},
    "pdu_session_id": {"description": "Protocol Data Unit session ID."},
    "pod_name": {"description": "Container name."},
    "pod_namespace": {"description": "Container namespace."},
    "policy_id": {"description": "Name of the SD-WAN policy."},
    "policy_name": {"description": "The name of the Decryption policy associated with the session."},
    "portal": {"description": "Global Protect Portal or Gateway that the user connected to."},
    "private_ip": {"type": "ip", "description": "Private IP address (v4) of the user that connected."},
    "private_ipv6": {"type": "ip", "description": "Private IP address (v6) of the user that connected."},
    "profile_name": {"description": "Data filtering profile name."},
    "protocol": {"description": "IP protocol associated with the session."},
    "proxy_type": {
        "description": "The Decryption proxy type, such as Forward for Forward Proxy, Inbound for Inbound "
                       "Inspection, No Decrypt for undecrypted traffic, Decryption Broker, GlobalProtect, "
                       "and so forth."},
    "public_ip": {"type": "ip", "description": "Public IP address (v4) of the user that connected."},
    "public_ipv6": {"type": "ip", "description": "Public IP address (v6) of the user that connected."},
    "quarantine_reason": {"description": "Quarantine reason."},
    "radio_access_technology": {"description": "Identifies the type of technology used for radio access."},
    "reason_data_filtering": {"description": "Reason for data filtering action."},
    "recipient_of_virus": {
        "description": "Identifies the recipient of an email that sandbox determined to be malicious when "
                       "it was analyzing an email link forwarded by the firewall."},
    "referer": {"description": "The web page URL identified in the HTTP REFERER header field."},
    "referer_fqdn": {"description": "The fully qualified domain name used in the HTTP REFERER header field."},
    "referer_port": {"type": "int", "description": "The port used in the HTTP REFERER header field."},
    "referer_protocol": {"description": "The protocol used in the HTTP REFERER header field."},
    "referer_url_path": {"description": "The URL path used in the HTTP REFERER header field."},
    "report_id": {"description": "Identifies the analysis requested from the sandbox (cloud or appliance)."},
    "risk_of_app": {
        "description": "Indicates how risky the application is from a network security perspective."},
    "root_cn": {"description": "The name of the root certificate authority."},
    "root_cn_len": {
        "description": "The length of the root CA's common name before truncation (if any).",
        "type": "int",
    },
    "root_status": {
        "description": "The status of the root certificate, for example, trusted, untrusted, or uninspected."
    },
    "rule_matched": {"description": "Name of the security policy rule that the network traffic matched."},
    "rule_matched_uuid": {
        "description": "Unique identifier for the security policy rule that the network traffic matched."},
    "sanctioned_state_of_app": {
        "description": "Indicates whether the application has been flagged as sanctioned by the firewall "
                       "administrator."},
    "sccp_calling_gt": {
        "description": "The Global Title (GT) specified in the called party address used for this SCCP "
                       "protocol message."},
    "sccp_calling_ssn": {
        "description": "The subsystem number (SSN) specified in the called party address used for this SCCP "
                       "protocol message."},
    "sctp_cause_code": {"description": "The error cause code found in the SCTP message."},
    "sctp_chunk_type": {"description": "Type of information contained in the SCTP data chunk."},
    "sctp_filter": {"description": "The SCTP filter that the firewall applied to this network traffic."},
    "sdwan_FEC_ratio": {"description": "SDWAN forward error correction (FEC) ratio."},
    "sdwan_cluster": {"description": "Name of the SD-WAN cluster."},
    "sdwan_cluster_type": {"description": "Type of SD-WAN cluster. Either mesh or hub-spoke."},
    "sdwan_device_type": {"description": "Type of SD-WAN device. Either hub or branch."},
    "sdwan_site": {"description": "Name of the SD-WAN site."},
    "sender_of_virus": {
        "description": "Identifies the sender of an email that sandbox determined to be malicious when it "
                       "was analyzing an email link forwarded by the firewall."},
    "sequence_no": {
        "description": "The log entry identifier, which is incremented sequentially. Each log type has a "
                       "unique number space.",
        "type": "long"},
    "service_region": {"description": "Region where the service is deployed."},
    "sess_owner_rt_midx": {"description": "Unknown field. No information is available at this time."},
    "session_end_reason": {"description": "The reason a session terminated."},
    "session_id": {
        "description": "Identifies the firewall's internal identifier for a specific network session."},
    "session_start_time": {
        "description": "Time when the session was established. This string contains a timestamp value that "
                       "is the number of microseconds since the Unix epoch."},
    "session_tracker": {"description": "Unknown field. No information is available at this time."},
    "severity": {"description": "Severity as defined by the platform."},
    "sig_flags": {"description": "Internal use only."},
    "sni": {"description": "The hostname of the server that the client is trying to contact."},
    "sni_len": {
        "description": "The length of the server name indication (SNI), which is the hostname of the server "
                       "that the client is trying to reach. This is the full length of the SNI before any "
                       "truncation might have occurred.",
        "type": "int",
    },
    "source": {"description": "Source."},
    "source_device_category": {"description": "Category of the device from which the session originated."},
    "source_device_class": {"description": "Source device class."},
    "source_device_host": {"description": "Hostname of the device from which the session originated."},
    "source_device_mac": {"description": "MAC Address of the device from which the session originated."},
    "source_device_model": {"description": "Model of the device from which the session originated."},
    "source_device_os": {"description": "Source device OS type."},
    "source_device_os_family": {"description": "OS family of the device from which the session originated."},
    "source_device_os_version": {"description": "OS version of the device from which the session originated."},
    "source_device_profile": {"description": "Profile of the device from which the session originated."},
    "source_device_vendor": {"description": "Vendor of the device from which the session originated."},
    "source_dynamic_address_group": {
        "description": "The dynamic address group that Device-ID identifies as the source of the traffic."},
    "source_edl": {
        "description": "The name of the external dynamic list that contains the source IP address of the "
                       "traffic."},
    "source_ip": {"type": "ip", "description": "Original source IP address"},
    "source_ip_v6": {"type": "ip", "description": "Source from which mapping information is collected."},
    "source_location": {"description": "Source country or internal region for private addresses."},
    "source_port": {"type": "int", "description": "Source port utilized by the session."},
    "source_region": {"description": "Region of the Gateway (or User) that connected."},
    "source_user": {"description": "The username that initiated the network traffic."},
    "source_user_info_domain": {"description": "Domain to which the Source User belongs."},
    "source_user_info_name": {
        "description": "The Source User. That is, the username that initiated the network traffic."},
    "source_user_info_uuid": {"description": "Unique identifier assigned to the Source User."},
    "source_uuid": {
        "description": "Identifies the source universal unique identifier for a guest virtual machine in "
                       "the VMware NSX environment."},
    "ssl_response_time": {"description": "SSL Response Time in milliseconds."},
    "stage": {"description": "Name of the stage in the GlobalProtect connection workflow."},
    "standard_ports_of_app": {"description": "Standard Ports of App."},
    "status": {"description": "The status (success or failure) of the event."},
    "stream_id": {"description": "Identifies the firewall's internal identifier for the SCTP stream."},
    "sub_type": {"description": "Identifies the log subtype."},
    "subject_of_email": {
        "description": "Identifies the subject of an email that the sandbox determined to be malicious when "
                       "it was analyzing an email link forwarded by the firewall."},
    "tag_name": {"description": "The tag mapped to the user."},
    "technology_of_app": {"description": "The networking technology used by the identified application."},
    "template": {
        "description": "The ID and name of the template/template stack to which the firewall belonged where "
                       "the log was generated."},
    "threat_category": {"description": "Threat category of the detected threat."},
    "threat_id": {"description": "Numerical identifier for the threat type."},
    "threat_name": {"description": "Palo Alto Networks textual identifier for the threat."},
    "threat_name_firewall": {"description": "Threat Name written by the firewall."},
    "time_generated": {
        "description": "Time when the log was generated on the firewall's data plane. This string contains "
                       "a timestamp value that is the number of microseconds since the Unix epoch."},
    "time_generated_high_res": {
        "description": "Time the log was generated in data plane with millisec granularity in format "
                       "YYYY-MM-DDTHH:MM:SS[.DDDDDD]Z.",
        "type": "timestamp"},
    "time_received_mp": {
        "description": "Time the log was received in the management plane in format YYYY-MM-DDTHH:MM:SS["
                       ".DDDDDD]Z.", "type": "pa_timestamp"},
    "timestamp_device_identification": {
        "description": "Time the device was identified in format YYYY-MM-DDTHH:MM:SS[.DDDDDD]Z.",
        "type": "pa_timestamp"},
    "tls_auth": {"description": "TLS hash algorithm."},
    "tls_enc_algorithm": {
        "description": "The algorithm used to encrypt the session data, such as AES-128-CBC, AES-256-GCM, "
                       "and so forth."},
    "tls_keyxchange": {
        "description": "Algorithm used to perform the key exchange. Possible values are: RSA, DHE, ECDHE, "
                       "TLS1.3"},
    "tls_version": {
        "description": "Version of TLS used for the encrypted session represented as major.minor.patch.build."
    },
    "to_zone": {"description": "Networking zone to which the traffic was sent."},
    "total_time_elapsed": {"description": "Total time taken for the network session to complete."},
    "tpadding": {"description": "For internal use only."},
    "tunnel": {"description": "Tunnel Type i.e. SSL or VPN."},
    "tunnel_cause_code": {"description": "GTP cause value in log responses."},
    "tunnel_endpoint_id_1": {
        "description": "Identifies the GTP tunnel in the network node. TEID1 is the first TEID in the GTP "
                       "messages."},
    "tunnel_endpoint_id_2": {
        "description": "Identifies the GTP tunnel in the network node. TEID2 is the second TEID in the GTP "
                       "messages."},
    "tunnel_event_code": {"description": "Event code describing the GTP event."},
    "tunnel_event_type": {"description": "Identifies the GTP event type for the traffic."},
    "tunnel_inspection_rule": {"description": "Name of the security policy rule in effect for the session."},
    "tunnel_interface": {"description": "3GPP interface from which a GTP message is received."},
    "tunnel_message_type": {"description": "Identifies the GTP message type."},
    "tunnel_remote_imsi_id": {
        "description": "International Mobile Subscriber Identity (IMSI) of a remote user at the end of an "
                       "S11-U tunnel."},
    "tunnel_remote_user_ip": {"type": "ip",
                              "description": "IP address of a remote user at the end of an S11-U tunnel."},
    "tunnel_sessions_closed": {"description": "Number of completed/closed sessions created."},
    "tunnel_sessions_created": {"description": "Number of inner sessions created."},
    "tunneled_app": {"description": "For internal use only."},
    "tunnelid_imsi": {
        "description": "ID of the tunnel being inspected or the International Mobile Subscriber Identity ("
                       "IMSI) ID of the mobile user."},
    "ug_flags": {
        "description": "Bit field used to indicate the status of user and group information when the "
                       "next-generation firewall is performing an IP-to-username mapping."},
    "uri": {"description": "The Uniform Resource Identifier (URI) used in the web request."},
    "url_category": {"description": "URL category associated with the session."},
    "url_category_list": {"description": "The list of associated URL categories."},
    "url_domain": {"description": "The name of the internet domain that was visited in this session."},
    "url_idx": {"description": "The column that correlates the traffic, url, and sandbox logs."},
    "user": {"description": "End user being authenticated."},
    "user_agent": {
        "description": "The User Agent field specifies the web browser that the user used to access the URL."
    },
    "user_group_found": {"description": "Indicates whether the user could be mapped to a group."},
    "user_identified_by_source_as": {"description": "The user name as sent by the data source."},
    "users": {"description": "Source/Destination user. If neither is available, source_ip is used."},
    "uuid": {"description": "UUID."},
    "vendor_name": {"description": "Identifies the vendor that produced the data."},
    "vendor_severity": {"description": "Severity associated with the event."},
    "verdict": {"description": "The verdict on the file sent for virus analysis."},
    "verification_tag_1": {"description": "The verification tag set for the SCTP packet."},
    "verification_tag_2": {"description": "The verification tag set for the SCTP packet."},
    "vpadding": {"description": "For internal use only."},
    "vsys": {
        "description": "String representation of the unique identifier for a virtual system on a Palo Alto "
                       "Networks firewall."},
    "vsys_id": {"description": "A unique identifier for a virtual system on a Palo Alto Networks firewall."},
    "vsys_name": {"description": "The name of the virtual system associated with the network traffic."},
    "xff": {"type": "ip", "description": "The IP address of the user who requested the web page."},
    "xff_ip": {"type": "ip", "description": "X-Forwarded-For IP."},
}


def get_paloalto_field_type(name: str) -> str:
    return __paloalto_log_fields__.get(name, {}).get("type", "string")


def get_paloalto_field_spark_type(name: str) -> str:
    s = get_paloalto_field_type(name)
    if s == "pa_timestamp":
        return "string"
    return get_spark_type_for_data_type(s)