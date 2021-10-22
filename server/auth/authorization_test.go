package auth_test

import (
	"custodian/server/auth"
	"net/http"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Auth Methods", func() {
	os.Setenv("SERVICE_DOMAIN", "TEST_RUN")
	os.Setenv("SERVICE_AUTH_SECRET", "123")

	It("Must create service token", func() {
		token, err := auth.GetServiceToken()

		Expect(err).To(BeNil())
		Expect(token).To(Equal("TEST_RUN:qhHHuvD7m2FMzbDZVLFB0_c2SIE"))
	})

	It("Must check service token", func() {
		token := "TEST_RUN:qhHHuvD7m2FMzbDZVLFB0_c2SIE"

		verified := auth.CheckServiceToken(token)
		Expect(verified).To(BeTrue())
	})

	It("Must authenticate Service user", func() {
		authenticator := auth.TroodAuthenticator{}

		testRequest := &http.Request{
			Header: map[string][]string{
				"Authorization": {"Service TEST_RUN:qhHHuvD7m2FMzbDZVLFB0_c2SIE"},
			},
		}

		user, abac, err := authenticator.Authenticate(testRequest)

		Expect(err).To(BeNil())

		expectedAbac := make(map[string]interface{})
		Expect(abac).To(Equal(expectedAbac))

		Expect(user.Type).To(Equal("service"))
	})
})