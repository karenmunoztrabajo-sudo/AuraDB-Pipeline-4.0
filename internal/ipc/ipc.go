package ipc

type Context struct {
	TenantID      string
	UserID        string
	Roles         []string
	PolicyVersion string
	RequestID     string
	AuditNonce    string
}