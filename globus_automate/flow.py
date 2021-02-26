from keys import smtp_user, smtp_pass

sender_email = "cfde-submission@nih-cfde.org"
admin_email = "nick@globus.org"
smtp_hostname = "email-smtp.us-east-1.amazonaws.com"

failure_text = ("'Your CFDE submission (' + submission_id + ') failed to ingest into DERIVA "
                "with this error:\\n' + error")

success_email_template = ("Your CFDE submission ($submission_id) has been successfully ingested, "
                          "and can be viewed here: $submission_link \n Thank you.")

test_sub_success_template = ("Your test CFDE submission ($submission_id) was successfully tested with DERIVA. "
                             "No errors were encountered.")

full_submission_flow_def = {
    "api_version": "1.0",
    "definition": {
        "StartAt": "ChooseTransfer",
        "States": {
            "ChooseTransfer": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.source_endpoint_id",
                    "BooleanEquals": False,
                    "Next": "CreateNoTransferVars"
                }],
                "Default": "TransferData"
            },
            "TransferData": {
                "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
                "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "source_endpoint_id.$": "$.source_endpoint_id",
                    "destination_endpoint_id.$": "$.cfde_ep_id",
                    "transfer_items": [{
                        "source_path.$": "$.source_path",
                        "destination_path.$": "$.cfde_ep_path",
                        "recursive.$": "$.is_directory"
                    }]
                },
                "ResultPath": "$.TransferResult",
                "Type": "Action",
                "WaitTime": 86400,
                "Next": "CreateTransferVars"
            },
            "CreateTransferVars": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/expression_eval",
                "ActionScope": "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "expressions": [{
                        "expression": "base_url + dest_path",
                        "arguments": {
                            "base_url.$": "$.cfde_ep_url",
                            "dest_path.$": "$.cfde_ep_path"
                        },
                        "result_path": "data_url"
                    }]
                },
                "ResultPath": "$.data_url",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "DerivaIngest"
            },
            "CreateNoTransferVars": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/expression_eval",
                "ActionScope": "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "expressions": [{
                        "expression": "data_url",
                        "arguments": {
                            "data_url.$": "$.data_url"
                        },
                        "result_path": "data_url"
                    }]
                },
                "ResultPath": "$.data_url",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "DerivaIngest"
            },
            "DerivaIngest": {
                "ActionScope": 'https://auth.globus.org/scopes/21017803-059f-4a9b-b64c-051ab7c1d05d/demo',
                "ActionUrl": None,
                #"ExceptionOnActionFailure": True,
                #TODO
                # Require new catalog, set ACL to author only
                "Parameters": {
                    "data_url.$": "$.data_url.details.data_url",
                    "operation": "ingest",
                    "globus_ep.$": "$.cfde_ep_id",
                    # "server": "demo.derivacloud.org",
                    # "submission_id.$": ,
                    "dcc_id.$": "$.dcc_id",
                    "test_sub.$": "$.test_sub"
                },
                "ResultPath": "$.DerivaIngestResult",
                "Type": "Action",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "CheckDerivaIngest"
            },
            "CheckDerivaIngest": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.DerivaIngestResult.details.error",
                    "BooleanEquals": False,
                    "Next": "CheckTestSub"
                }],
                "Default": "FailDerivaIngest"
            },
            "CheckTestSub": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.test_sub",
                    "BooleanEquals": True,
                    "Next": "TestSubEmail"
                }],
                "Default": "EmailSuccess"
            },
            "EmailSuccess": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/notification/notify",
                "ActionScope": "https://auth.globus.org/scopes/helloworld.actions.automate.globus.org/notification_notify",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    # "body_mimetype": "",
                    "body_template": success_email_template,
                    "body_variables": {
                        "submission_id.$": "$.DerivaIngestResult.details.submission_id",
                        "submission_link.$": "$.DerivaIngestResult.details.submission_link"
                    },
                    "destination.$": "$._context.email",
                    # "notification_method": "",
                    # "notification_priority": "low",
                    "send_credentials": [{
                        # "credential_method": "",
                        "credential_type": "smtp",
                        "credential_value": {
                            "hostname": smtp_hostname,
                            "username": smtp_user,
                            "password": smtp_pass
                        }
                    }],
                    "__Private_Parameters": ["send_credentials"],
                    "sender": sender_email,
                    "subject": "Submission Succeeded"
                },
                "ResultPath": "$.EmailSuccessResult",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "FlowSuccess"
            },
            "FlowSuccess": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/expression_eval",
                "ActionScope": "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "expressions": [{
                        "expression": "submission_link",
                        "arguments": {
                            "submission_link.$": "$.DerivaIngestResult.details.submission_link"
                        },
                        "result_path": "submission_link"
                    }, {
                        "expression": ("'Submission Flow succeeded. Your submission ID is ' + str(submission_id) + "
                                       "', and your submission can be viewed at this link: ' + submission_link"),
                        "arguments": {
                            "submission_link.$": "$.DerivaIngestResult.details.submission_link",
                            "submission_id.$": "$.DerivaIngestResult.details.submission_id"
                        },
                        "result_path": "message"
                    }]
                },
                "ResultPath": "$.SuccessState",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "FinishFlow"
            },
            "TestSubEmail": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/notification/notify",
                "ActionScope": "https://auth.globus.org/scopes/helloworld.actions.automate.globus.org/notification_notify",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    # "body_mimetype": "",
                    "body_template": test_sub_success_template,
                    "body_variables": {
                        "action_id.$": "$._context.action_id"
                    },
                    "destination.$": "$._context.email",
                    # "notification_method": "",
                    # "notification_priority": "low",
                    "send_credentials": [{
                        # "credential_method": "",
                        "credential_type": "smtp",
                        "credential_value": {
                            "hostname": smtp_hostname,
                            "username": smtp_user,
                            "password": smtp_pass
                        }
                    }],
                    "__Private_Parameters": ["send_credentials"],
                    "sender": sender_email,
                    "subject": "Test Submission Succeeded"
                },
                "ResultPath": "$.TestSubSuccessResult",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "TestSubSuccess"
            },
            "TestSubSuccess": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/expression_eval",
                "ActionScope": "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "expressions": [{
                        "expression": "'Test Submission Flow succeeded. No DERIVA errors were encountered.'",
                        "result_path": "message"
                    }]
                },
                "ResultPath": "$.SuccessState",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "FinishFlow"
            },
            "FailDerivaIngest": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/expression_eval",
                "ActionScope": "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "expressions": [{
                        "expression": failure_text,
                        "arguments": {
                            "submission_id.$": "$.DerivaIngestResult.details.submission_id",
                            "error.$": "$.DerivaIngestResult.details.error"
                        },
                        "result_path": "error"
                    }]
                },
                "ResultPath": "$.FailureState",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "FailDerivaIngest2"
            },
            "FailDerivaIngest2": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/notification/notify",
                "ActionScope": "https://auth.globus.org/scopes/helloworld.actions.automate.globus.org/notification_notify",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    # "body_mimetype": "",
                    "body_template.$": "$.FailureState.details.error",
                    "destination.$": "$._context.email",
                    # "notification_method": "",
                    # "notification_priority": "low",
                    "send_credentials": [{
                        # "credential_method": "",
                        "credential_type": "smtp",
                        "credential_value": {
                            "hostname": smtp_hostname,
                            "username": smtp_user,
                            "password": smtp_pass
                        }
                    }],
                    "__Private_Parameters": ["send_credentials"],
                    "sender": sender_email,
                    "subject": "Submission Failed to Ingest"
                },
                "ResultPath": "$.FailDerivaIngestResult",
                "WaitTime": 86400,
                "Catch": [{
                  "ErrorEquals": ["States.ALL"],
                  "Next": "ErrorState"
                }],
                "Next": "FinishFlow"
            },
            "ErrorState": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/notification/notify",
                "ActionScope": "https://auth.globus.org/scopes/helloworld.actions.automate.globus.org/notification_notify",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    # "body_mimetype": "",
                    "body_template.=": ("A CFDE Flow has encountered an error. Please contact your administrator "
                                        "and provide the UUID below to aid in debugging\n"
                                        "Flow instance ID: `$._context.action_id`.\n\n"),
                    "destination": admin_email,
                    # "notification_method": "",
                    # "notification_priority": "low",
                    "send_credentials": [{
                        # "credential_method": "",
                        "credential_type": "smtp",
                        "credential_value": {
                            "hostname": smtp_hostname,
                            "username": smtp_user,
                            "password": smtp_pass
                        }
                    }],
                    "__Private_Parameters": ["send_credentials"],
                    "sender": sender_email,
                    "subject": "Submission Failed to Ingest"
                },
                "ResultPath": "$.ErrorStateResult",
                "WaitTime": 86400,
                "Next": "ErrorFlowLog"
            },
            "ErrorFlowLog": {
                "Type": "Action",
                "ActionUrl": "https://actions.globus.org/expression_eval",
                "ActionScope": "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression",
                "ExceptionOnActionFailure": True,
                "Parameters": {
                    "expressions": [{
                        "expression": "text",
                        "arguments": {
                            "text": ("A service error has occurred, and the CFDE team has been notified. "
                                     "You may be contacted with additional details.")
                        },
                        "result_path": "error"
                    }]
                },
                "ResultPath": "$.ErrorState",
                "WaitTime": 86400,
                "Next": "FinishFlow"
            },
            "FinishFlow": {
                "Type": "Pass",
                "End": True
            },
        }
    },
    "description": ("Run the CFDE submission flow."),
    # runnable_by and visible_to are dynamically set with values in each Deriva app instance
    # when running the deploy.py script.
    # "runnable_by": ["urn:globus:groups:id:a437abe3-c9a4-11e9-b441-0efb3ba9a670"],  # CFDE DERIVA Demo
    # "visible_to": ["urn:globus:groups:id:a437abe3-c9a4-11e9-b441-0efb3ba9a670"]  # CFDE DERIVA Demo
    "synchronous": False,
    "title": "CFDE Submission",
    "types": ["Action", "Choice"],
}