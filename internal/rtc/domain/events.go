package domain

const (
	EventVoiceChannelJoined    = "VoiceChannelJoined"
	EventVoiceChannelLeft      = "VoiceChannelLeft"
	EventMicrophoneMuted       = "MicrophoneMuted"
	EventMicrophoneUnmuted     = "MicrophoneUnmuted"
	EventCameraEnabled         = "CameraEnabled"
	EventCameraDisabled        = "CameraDisabled"
	EventRtcTokenIssued        = "RtcTokenIssued"
	CommandJoinVoiceChannel    = "JoinVoiceChannel"
	CommandLeaveVoiceChannel   = "LeaveVoiceChannel"
	CommandMuteSelf            = "MuteSelf"
	CommandUnmuteSelf          = "UnmuteSelf"
	CommandEnableCamera        = "EnableCamera"
	CommandDisableCamera       = "DisableCamera"
	CommandIssueRtcToken       = "IssueRtcToken"
	CommandTerminateVoiceState = "TerminateVoiceSession"
)
