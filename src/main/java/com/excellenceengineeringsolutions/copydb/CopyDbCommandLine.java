package com.excellenceengineeringsolutions.copydb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class CopyDbCommandLine implements CommandLineRunner
{

    private static final Logger log = LoggerFactory.getLogger(CopyDbCommandLine.class);

    @Autowired
    CopyDbService copyDbService;

    @Override
    public void run(String... args) {
        copyDbService.copy();
    }
}
