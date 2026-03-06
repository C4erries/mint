package domain

const (
	CommandCreateWorkspace = "CreateWorkspace"
	CommandCreateChannel   = "CreateChannel"
	CommandJoinWorkspace   = "JoinWorkspace"
	CommandBanMember       = "BanMember"

	EventWorkspaceCreated = "WorkspaceCreated"
	EventChannelCreated   = "ChannelCreated"
	EventMemberJoined     = "MemberJoinedWorkspace"
	EventMemberBanned     = "MemberBanned"
)
