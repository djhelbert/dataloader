package com.racing.dataloader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Util {
    public List<Organizer> readFile() {
        final List<Organizer> organizers = new ArrayList<>();

        try {
            final URL resource = getClass().getResource("/data.csv");
            final List<String> lines = Files.readAllLines(Paths.get(resource.toURI()));
            int count = 0;

            for(String line : lines) {
                if(count > 0) { // Ignore header
                    String[] parts = line.split(",");
                    Organizer org = new Organizer();
                    org.setId(new Integer(parts[0]));
                    org.setName(parts[1]);
                    org.setDescription(parts[2]);
                    org.setRaceType(parts[3]);
                    org.setUrl(parts[4]);

                    String[] states = parts[5].split(" ");
                    org.setStates(Arrays.asList(states));

                    organizers.add(org);
                }
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return organizers;
    }
}
