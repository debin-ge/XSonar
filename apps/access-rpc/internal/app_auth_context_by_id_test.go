package internal

import (
	"context"
	"testing"

	"xsonar/pkg/xlog"
)

func TestGetAppAuthContextByIDReturnsAppSnapshot(t *testing.T) {
	svc := newService(xlog.NewStdout("access-rpc-test"))

	tenantData, tenantErr := svc.createTenant(context.Background(), createTenantRequest{Name: "Acme"})
	if tenantErr != nil {
		t.Fatalf("createTenant returned error: %v", tenantErr)
	}
	tenant := tenantData.(*tenant)

	appData, appErr := svc.createTenantApp(context.Background(), createTenantAppRequest{
		TenantID:   tenant.ID,
		Name:       "Acme App",
		DailyQuota: 100,
		QPSLimit:   10,
	})
	if appErr != nil {
		t.Fatalf("createTenantApp returned error: %v", appErr)
	}
	app := appData.(*tenantApp)

	got, getErr := svc.getAppAuthContextByID(context.Background(), getAppAuthContextByIDRequest{AppID: app.ID})
	if getErr != nil {
		t.Fatalf("getAppAuthContextByID returned error: %v", getErr)
	}

	payload := got.(map[string]any)
	if payload["tenant_id"] != tenant.ID {
		t.Fatalf("expected tenant_id %q, got %#v", tenant.ID, payload["tenant_id"])
	}
	if payload["app_id"] != app.ID {
		t.Fatalf("expected app_id %q, got %#v", app.ID, payload["app_id"])
	}
	if payload["app_secret"] != app.AppSecret {
		t.Fatalf("expected app_secret to be preserved, got %#v", payload["app_secret"])
	}
}
