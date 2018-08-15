package com.excellenceengineeringsolutions.copydb;

public class SpannerProperties {

    protected String project;

    protected String instance;

    protected String database;

    protected String credentialsFile;

    protected int minSessions = 100;

    protected int maxSessions = 2000;

    protected int maxIdleSessions = 5;

    protected int keepAliveIntervalMinutes;

    protected boolean createNew;

    protected String[] ignore;

    public String getProject()
    {
        return project;
    }

    public String getInstance()
    {
        return instance;
    }

    public String getCredentialsFile()
    {
        return credentialsFile;
    }

    public int getMinSessions()
    {
        return minSessions;
    }

    public int getMaxSessions()
    {
        return maxSessions;
    }

    public int getMaxIdleSessions()
    {
        return maxIdleSessions;
    }

    public int getKeepAliveIntervalMinutes()
    {
        return keepAliveIntervalMinutes;
    }

    public String getDatabase()
    {
        return database;
    }

    public boolean isCreateNew()
    {
        return createNew;
    }

    public String[] getIgnore()
    {
        return ignore;
    }
}
