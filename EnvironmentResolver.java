package com.elevancehealth.dckr.microsvc.aksgbdsoamembereligibility;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EnvironmentResolver {
	private static Properties envIdProperties;

	static {
		try (InputStream input = EnvironmentResolver.class.getClassLoader().getResourceAsStream("envId.properties")) {
			if (input == null) {
				throw new IllegalStateException("Sorry, unable to find envId.properties");
			}
			envIdProperties = new Properties();
			envIdProperties.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static String getEnvProperty(String key) {
		if (envIdProperties == null) {
			throw new IllegalStateException("Properties have not been initialized.");
		}
		return envIdProperties.getProperty(key);
	}
}