//
// Created by 杜建璋 on 25-8-16.
//

/*
 * SELECT count(DISTINCT pid)
 * FROM diagnosis as d, medication as m
 * on d.pid = m.pid
 * WHERE d.diag = hd
 * AND m.med = aspirin
 * AND d.time <= m.time
 */
int main(int argc, char *argv[]) {

}
