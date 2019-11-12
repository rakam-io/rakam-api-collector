/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

public class LicenseConfig
{
    private String project = "rakam-bi-bundle";
    private String serviceAccountJson;
    private String keyRing = "rakam-bi-on-prem";
    private String keyRingLocation = "global";
    private String keyName;

    @Config("license.project")
    public void setProject(String project) {
        this.project = project;
    }

    @Config("license.service-account-json")
    public void setServiceAccountJson(String serviceAccountJson) {
        this.serviceAccountJson = serviceAccountJson;
    }

    public void setKeyRing(String keyRing) {
        this.keyRing = keyRing;
    }

    @Config("license.keyring-location")
    public void setKeyRingLocation(String keyRingLocation) {
        this.keyRingLocation = keyRingLocation;
    }

    @Config("license.keyname")
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public String getProject() {
        return project;
    }

    public String getServiceAccountJson() {
        return serviceAccountJson;
    }

    public String getKeyRing() {
        return keyRing;
    }

    public String getKeyRingLocation() {
        return keyRingLocation;
    }

    public String getKeyName() {
        return keyName;
    }
}
